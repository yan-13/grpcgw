package grpcgw

import (
    "google.golang.org/grpc"
    "google.golang.org/grpc/connectivity"
)

//conn
func (p *Gateway) getConn(serverAddr string) (conn *grpc.ClientConn, err error) {
    client, ok := p.isConnReady(serverAddr)
    if !ok {
        p.grpcClientPool.Lock()
        defer p.grpcClientPool.Unlock()

        //再检查一次
        client, ok = p.isConnReady(serverAddr)
        if ok {
            return client.conn, nil
        }

        //new conn
        newConn, err1 := grpc.Dial(serverAddr, grpc.WithInsecure())
        if err1 != nil {
            err = err1
            return
        }
        p.grpcClientPool.clientMap[serverAddr] = grpcClient{
            firstConn: true,
            conn:      newConn,
        }
    }
    conn = p.grpcClientPool.clientMap[serverAddr].conn
    return
}

func (p *Gateway) isConnReady(serverAddr string) (c grpcClient, isReady bool) {
    client, ok := p.grpcClientPool.clientMap[serverAddr]
    if ok && client.firstConn {
        connStatus := client.conn.GetState()
        if connStatus == connectivity.Idle || connStatus == connectivity.Ready {
            return client, true
        }
    }
    return
}
