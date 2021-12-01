package main

import (
    "errors"
    "github.com/hashicorp/consul/api"
    "math/rand"
    "time"
)

//connect consul
func (p *Gateway) connectConsul() error {
    config := api.DefaultConfig()
    config.Address = p.consulAddr
    client, err := api.NewClient(config)
    if err != nil {
        return err
    }
    p.consulClient = client
    return nil
}

//发现微服务
func (p *Gateway) discover(serviceName string) (ip string, port int, err error) {
    var lastIndex uint64
    services, metainfo, err1 := p.consulClient.Health().Service(serviceName, "", true, &api.QueryOptions{
        WaitIndex: lastIndex,
    })
    if err1 != nil {
        err = err1
        return
    }
    lastIndex = metainfo.LastIndex
    if 0 == len(services) {
        err = errors.New("discover service got 0")
        return
    }
    rand.Seed(time.Now().Unix())
    s := services[rand.Intn(len(services))]
    ip = s.Service.Address
    port = s.Service.Port
    return
}
