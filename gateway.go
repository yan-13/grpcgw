package grpcgw

import (
    "bytes"
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "github.com/golang/protobuf/jsonpb"
    "github.com/golang/protobuf/proto"
    "github.com/golang/protobuf/protoc-gen-go/descriptor"
    "github.com/hashicorp/consul/api"
    "github.com/jhump/protoreflect/desc"
    "github.com/jhump/protoreflect/desc/builder"
    "github.com/jhump/protoreflect/dynamic"
    "google.golang.org/grpc"
    "google.golang.org/grpc/metadata"
    "io/ioutil"
    "net/http"
    "strconv"
    "sync"
)

type Gateway struct {
    consulAddr     string
    apiProtoDir    string
    consulClient   *api.Client
    serviceCache   map[string]grpcService
    grpcClientPool grpcClientPool
}

type grpcClientPool struct {
    sync.Mutex
    clientMap map[string]grpcClient
}

type grpcClient struct {
    firstConn bool
    conn      *grpc.ClientConn
}

func NewGateway(consulAddr string, apiProtoDir string) (g *Gateway, err error) {
    g = &Gateway{consulAddr: consulAddr, apiProtoDir: apiProtoDir}
    //service cache
    g.serviceCache = make(map[string]grpcService)
    arr, err1 := g.ReloadServices()
    if err1 != nil {
        err = err1
        return
    }
    if 0 == len(arr) {
        err = errors.New("there are no services in api proto dir")
        return
    }
    //consul
    err = g.connectConsul()
    if err != nil {
        return
    }

    //grpc client cache
    g.grpcClientPool.clientMap = make(map[string]grpcClient)
    return
}

func (p *Gateway) GetServices() []string {
    arr := make([]string, 0)
    for k := range p.serviceCache {
        arr = append(arr, k)
    }
    return arr
}

func (p *Gateway) Handle(r *http.Request, withMeta map[string]string) (res string, err error) {
    path := r.URL.Path

    //route
    serviceName, h, method, err1 := p.resolvePath(path)
    if err1 != nil {
        err = err1
        return
    }

    //conn
    ip, port, err1 := p.discover(serviceName)
    if err1 != nil {
        err = err1
        return
    }
    conn, err1 := p.getConn(fmt.Sprintf("%s:%d", ip, port))
    if err1 != nil {
        err = err1
        return
    }

    //ctx
    ctx := context.Background()
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()

    //with metadata
    if len(withMeta) > 0 {
        md := metadata.New(withMeta)
        ctx = metadata.NewOutgoingContext(ctx, md)
    }

    //in and out
    in, out, err1 := buildInAndOut(method, h.HttpMethod, r)
    if err1 != nil {
        err = err1
        return
    }

    //api res
    apiRes := builder.NewMessage("apiRes")
    apiRes.AddField(builder.NewField("code", builder.FieldTypeInt32()))
    apiRes.AddField(builder.NewField("message", builder.FieldTypeString()))
    apiRes.AddField(builder.NewField("data", builder.FieldTypeImportedMessage(method.GetOutputType())))
    md, err1 := apiRes.Build()
    if err1 != nil {
        err = err1
        return
    }
    m := dynamic.NewMessage(md)
    marshaler := jsonpb.Marshaler{OrigName: true, EmitDefaults: true}

    //call
    reqPath := fmt.Sprintf("%s.%s/%s", serviceName, h.ServiceName, h.MethodName)
    err = conn.Invoke(ctx, reqPath, in, out)
    if err != nil {
        //todo grpc的error code是固定的, 暂时设code为1
        m.SetFieldByName("code", int32(1))
        m.SetFieldByName("message", err.Error())
    } else {
        m.SetFieldByName("data", out)
    }
    return marshaler.MarshalToString(m)
}

//分析要请求的grpc method
func (p *Gateway) resolvePath(path string) (serviceName string, h handler, method *desc.MethodDescriptor, err error) {
    //route
    serviceName, grpcPath, err1 := route(path)
    if err1 != nil {
        err = err1
        return
    }

    gs, ok := p.serviceCache[serviceName]
    if !ok {
        err = errors.New(fmt.Sprintf("service %s not registed", serviceName))
        return
    }

    h, ok = gs.Router[grpcPath]
    if !ok {
        err = errors.New(fmt.Sprintf("service %s has no route for path %s", serviceName, grpcPath))
        return
    }

    ss, ok := gs.Services[h.ServiceName]
    if !ok {
        err = errors.New(fmt.Sprintf("service %s has no sub service %s", serviceName, h.ServiceName))
        return
    }

    method, ok = ss.Methods[h.MethodName]
    if !ok {
        err = errors.New(fmt.Sprintf("service %s has no method %s", serviceName, h.MethodName))
        return
    }
    return serviceName, h, method, nil
}

//构造grpc的in和out
func buildInAndOut(method *desc.MethodDescriptor, httpMethod string, r *http.Request) (proto.Message, proto.Message, error) {
    inType := method.GetInputType()
    outType := method.GetOutputType()
    f := dynamic.NewMessageFactoryWithDefaults()
    in := f.NewMessage(inType)
    out := f.NewMessage(outType)

    var (
        jsonBytes []byte
        err       error
    )
    if "GET" == httpMethod {
        paramMap := make(map[string]interface{})
        query := r.URL.Query()
        inFields := inType.GetFields()
        for _, v := range inFields {
            jsonName := v.GetName()
            param := query.Get(jsonName)
            fType := v.GetType()
            if fType == descriptor.FieldDescriptorProto_TYPE_INT32 || fType == descriptor.FieldDescriptorProto_TYPE_INT64 {
                paramInt, err := strconv.Atoi(param)
                if err != nil {
                    paramInt = 0
                }
                paramMap[jsonName] = paramInt
            } else {
                //descriptor.FieldDescriptorProto_TYPE_STRING
                //其他类型暂时不考虑
                paramMap[jsonName] = param
            }
        }
        jsonBytes, err = json.Marshal(paramMap)
        if err != nil {
            err = errors.New("json encode request params error: " + err.Error())
        }
    } else {
        //POST
        jsonBytes, err = ioutil.ReadAll(r.Body)
        if err != nil {
            err = errors.New("json decode request body error: " + err.Error())
        }
    }
    if err != nil {
        return nil, nil, err
    }
    unmarshaler := jsonpb.Unmarshaler{AllowUnknownFields: true}
    err = unmarshaler.Unmarshal(bytes.NewReader(jsonBytes), in)
    if err != nil {
        err = errors.New("fill req message error: " + err.Error())
    }

    return in, out, err
}
