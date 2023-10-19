package client

import (
	"encoding/json"
	"errors"
	"net"
	"time"
)

// 初始化一个TCP连接
// 非连接池模式
func New(addr string) (*TCPConnection, error) {
	tcpConnection := &TCPConnection{
		ServerIndex:        0,
		CurrentServerIndex: 0,
		Status:             false,
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second*5)
	if err != nil {
		return tcpConnection, err
	}
	tcpConnection.CurrentServerAddr = addr
	tcpConnection.Conn = conn
	tcpConnection.Status = true
	return tcpConnection, nil
}

// 发生及接收消息
func (st *TCPConnection) SendAndReceive(message any, toJson bool) ([]byte, error) {

	if !st.Status {
		return nil, errors.New("TCP 服务错误")
	}
	var messageByte []byte
	var err error
	if toJson {
		messageByte, err = json.Marshal(message)
		if err != nil {
			return nil, err
		}
	} else {
		messageString, ok := message.(string)
		if !ok {
			return nil, errors.New("消息数据应为字符串")
		}
		messageByte = []byte(messageString)
	}

	// 发送
	_, err = st.Conn.Write(messageByte)
	if err != nil {
		return nil, err
	}

	// 接收
	buf := make([]byte, 5120)
	n, err := st.Conn.Read(buf)
	if err != nil {
		return nil, err
	}
	buf = buf[0:n]

	return buf, nil
}

// 关闭连接
func (st *TCPConnection) Close() {
	st.Conn.Close()
}
