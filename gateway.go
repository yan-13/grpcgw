package grpcgw

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "github.com/golang/protobuf/proto"
    "github.com/golang/protobuf/protoc-gen-go/descriptor"
    "github.com/hashicorp/consul/api"
    "github.com/jhump/protoreflect/desc"
    "github.com/jhump/protoreflect/dynamic"
    "google.golang.org/grpc"
    "google.golang.org/grpc/metadata"
    "io/ioutil"
    "net/http"
    "strconv"
)

type Gateway struct {
    consulAddr   string
    apiProtoDir  string
    consulClient *api.Client
    serviceCache map[string]grpcService
}

func NewGateway(consulAddr string, apiProtoDir string) (g *Gateway, err error) {
    g = &Gateway{consulAddr: consulAddr, apiProtoDir: apiProtoDir}
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
    err = g.connectConsul()
    if err != nil {
        return
    }
    return
}

func (p *Gateway) GetServices() []string {
    arr := make([]string, 0)
    for k := range p.serviceCache {
        arr = append(arr, k)
    }
    return arr
}

func (p *Gateway) Handle(r *http.Request, withMeta map[string]string) (out proto.Message, err error) {
    path := r.URL.Path
    //route
    serviceName, grpcPath, err1 := route(path)
    if err1 != nil {
        err = err1
        return
    }

    gs, ok := p.serviceCache[serviceName]
    if !ok {
        err = errors.New("service not found")
        return
    }

    handler, ok := gs.Router[grpcPath]
    if !ok {
        err = errors.New("handler not found")
        return
    }

    reqPath := fmt.Sprintf("%s.%s/%s", serviceName, handler.ServiceName, handler.MethodName)
    ss, ok := gs.Services[handler.ServiceName]
    if !ok {
        err = errors.New("sub service not found")
        return
    }

    method, ok := ss.Methods[handler.MethodName]
    if !ok {
        err = errors.New("method not found")
        return
    }

    //conn
    ip, port, err1 := p.discover(serviceName)
    if err1 != nil {
        err = err1
        return
    }
    serverAddr := fmt.Sprintf("%s:%d", ip, port)
    conn, err1 := grpc.Dial(serverAddr, grpc.WithInsecure())
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
    in, out, err := buildInAndOut(method, handler.HttpMethod, r)

    //call
    err = conn.Invoke(ctx, reqPath, in, out)
    return
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
            jsonName := v.GetJSONName()
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
            err = errors.New("json encode params error: " + err.Error())
        }
    } else {
        jsonBytes, err = ioutil.ReadAll(r.Body)
        if err != nil {
            err = errors.New("json decode body error: " + err.Error())
        }
    }
    if err != nil {
        return nil, nil, err
    }
    err = json.Unmarshal(jsonBytes, in)
    if err != nil {
        err = errors.New("fill in message error: " + err.Error())
    }

    return in, out, err
}
