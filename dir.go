package main

import (
    "fmt"
    "io/ioutil"
    "strings"
)

//获取服务的包名
func (p *Gateway) getImportPath(serviceName string) string {
    return fmt.Sprintf("%s/%s", p.apiProtoDir, serviceName)
}

//获取路由配置
func (p *Gateway) getRouterConfigPath(serviceName string) string {
    return fmt.Sprintf("%s/router.yaml", p.getImportPath(serviceName))
}

//获取所有proto文件
func (p *Gateway) getProtoFiles(serviceName string) (arr []string, err error) {
    dir := p.getImportPath(serviceName)
    files, err1 := ioutil.ReadDir(dir)
    if err1 != nil {
        err = err1
        return
    }

    for _, v := range files {
        if !v.IsDir() {
            name := v.Name()
            nameArr := strings.Split(name, ".")
            if nameArr[len(nameArr)-1] == "proto" {
                arr = append(arr, name)
            }
        }
    }
    return
}
