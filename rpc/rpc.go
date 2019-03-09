package rpc

import (
	"net"
	"net/http"
)

type RPCContext interface {
	Close()
}

type HttpContext struct {
	Request  *http.Request
	Response http.ResponseWriter
}

func (c *HttpContext) Close() {
}

type TcpContext struct {
	Attachement interface{}
	Connection  net.Conn
}

func (c *TcpContext) Close() {
	c.Connection.Close()
}
