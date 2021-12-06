package grpcgw

import (
    "context"
    "errors"
    "fmt"
    "github.com/hashicorp/consul/api"
    "google.golang.org/grpc"
    "google.golang.org/grpc/health/grpc_health_v1"
    "log"
    "net"
    "strconv"
    "strings"
)

type GrpcServer struct {
    consulAddr  string
    serviceName string
    nodeId      string
    nodeIp      string
    port        int
    server      *grpc.Server
    silence     bool
}

// HealthImpl 健康检查实现
type HealthImpl struct{}

// Check 实现健康检查接口，这里直接返回健康状态，这里也可以有更复杂的健康检查策略，比如根据服务器负载来返回
func (h *HealthImpl) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
    return &grpc_health_v1.HealthCheckResponse{
        Status: grpc_health_v1.HealthCheckResponse_SERVING,
    }, nil
}

//Watch 这个没用，只是为了让HealthImpl实现RegisterHealthServer内部的interface接口
func (h *HealthImpl) Watch(req *grpc_health_v1.HealthCheckRequest, w grpc_health_v1.Health_WatchServer) error {
    return nil
}

func NewDefaultServer(consulAddr string, serviceName string, nodeId string, nodeIp string) GrpcServer {
    if "" == consulAddr || "" == serviceName || "" == nodeId || "" == nodeIp {
        log.Fatal("consulAddr, serviceName, nodeId, nodeIp must not be empty")
    }
    s := grpc.NewServer()
    grpc_health_v1.RegisterHealthServer(s, &HealthImpl{})
    return GrpcServer{
        consulAddr:  consulAddr,
        serviceName: serviceName,
        nodeId:      nodeId,
        nodeIp:      nodeIp,
        port:        0,
        server:      s,
    }
}

func (p *GrpcServer) SetSilence(b bool) {
    p.silence = b
}

func (p *GrpcServer) Serve() error {
    if p.port > 0 {
        return errors.New(fmt.Sprintf("server is already serving@%s:%d", p.nodeIp, p.port))
    }

    lis, err := net.Listen("tcp", ":0")
    if err != nil {
        return err
    }
    //port
    arr := strings.Split(lis.Addr().String(), ":")
    servicePort := arr[len(arr)-1]
    port, err := strconv.Atoi(servicePort)
    if err != nil {
        return err
    }
    p.port = port
    err = p.registerConsul()
    if err != nil {
        return err
    }

    //serve
    if !p.silence {
        log.Println("grpc server is running@" + servicePort)
    }
    return p.server.Serve(lis)
}

//向consul注册微服务
func (p *GrpcServer) registerConsul() error {
    config := api.DefaultConfig()
    config.Address = p.consulAddr
    client, err := api.NewClient(config)
    if err != nil {
        return err
    }

    reg := &api.AgentServiceRegistration{
        ID:      p.nodeId,      // 服务节点的名称
        Name:    p.serviceName, // 服务名称
        Port:    p.port,        // 服务端口
        Address: p.nodeIp,      // 服务 IP
        // 健康检查
        Check: &api.AgentServiceCheck{
            Interval:                       "3s",
            GRPC:                           fmt.Sprintf("%s:%d", p.nodeIp, p.port),
            DeregisterCriticalServiceAfter: "5s",
        },
    }
    err = client.Agent().ServiceRegister(reg)
    if err != nil {
        return err
    }
    if !p.silence {
        log.Println(fmt.Sprintf("consul registered, service name=%s, node id=%s", p.serviceName, p.nodeId))
    }
    return nil
}
