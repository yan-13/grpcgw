package grpcgw

import (
    "errors"
    "github.com/jhump/protoreflect/desc"
    "github.com/jhump/protoreflect/desc/protoparse"
    "io/ioutil"
)

type grpcService struct {
    Name       string
    ImportPath string
    Services   map[string]subService
    Router     map[string]handler
}

type subService struct {
    Name    string
    Methods map[string]*desc.MethodDescriptor
}

//重新加载服务
func (p *Gateway) ReloadServices() (arr []string, err error) {
    files, err1 := ioutil.ReadDir(p.apiProtoDir)
    if err1 != nil {
        err = err1
        return
    }
    for _, v := range files {
        if v.IsDir() {
            vName := v.Name()
            err1 = p.loadService(vName)
            if err1 != nil {
                err = err1
                return
            }
            arr = append(arr, vName)
        }
    }
    return
}

//加载服务
func (p *Gateway) loadService(serviceName string) error {
    s := grpcService{
        Name:       serviceName,
        ImportPath: p.getImportPath(serviceName),
        Services:   make(map[string]subService),
        Router:     make(map[string]handler),
    }

    //load file descriptor
    fds, err := p.getFds(serviceName)
    if err != nil {
        return err
    }
    for _, v := range fds {
        for _, vv := range v.GetServices() {
            sName := vv.GetName()
            subService := subService{
                Name:    vv.GetName(),
                Methods: make(map[string]*desc.MethodDescriptor),
            }
            for _, vvv := range vv.GetMethods() {
                subService.Methods[vvv.GetName()] = vvv
            }
            s.Services[sName] = subService
        }
    }

    //load router
    r, err := p.loadRouter(serviceName)
    if err != nil {
        return err
    }
    s.Router = r

    p.serviceCache[serviceName] = s
    return nil
}

//从api包中分析FileDescriptor
func (p *Gateway) getFds(serviceName string) (fds []*desc.FileDescriptor, err error) {
    arr, err1 := p.getProtoFiles(serviceName)
    if err1 != nil {
        err = err1
        return
    }
    if 0 == len(arr) {
        err = errors.New("no proto files found")
        return
    }
    parser := protoparse.Parser{ImportPaths: []string{p.getImportPath(serviceName)}, IncludeSourceCodeInfo: true}
    return parser.ParseFiles(arr...)
}
