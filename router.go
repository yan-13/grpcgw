package grpcgw

import (
    "errors"
    "gopkg.in/yaml.v2"
    "io/ioutil"
    "strings"
)

type yamlRouter struct {
    Services map[string]map[string][]string
}

type handler struct {
    HttpMethod  string
    ServiceName string
    MethodName  string
}

func (p *Gateway) loadRouter(serviceName string) (router map[string]handler, err error) {
    yamlBytes, err1 := ioutil.ReadFile(p.getRouterConfigPath(serviceName))
    if err1 != nil {
        err = err1
        return
    }
    var yr yamlRouter
    err = yaml.Unmarshal(yamlBytes, &yr)
    if err != nil {
        return
    }

    //build router
    router = make(map[string]handler)
    for k, v := range yr.Services {
        for kk, vv := range v {
            router[kk] = handler{
                HttpMethod:  vv[0],
                ServiceName: k,
                MethodName:  vv[1],
            }
        }
    }
    return
}

func route(path string) (serviceName string, grpcPath string, err error) {
    trimPath := strings.TrimLeft(path, "/")
    arr := strings.SplitN(trimPath, "/", 2)
    if 2 != len(arr) {
        err = errors.New("request path illegal: " + path)
        return
    }
    serviceName = arr[0]
    grpcPath = "/" + arr[1]
    return
}
