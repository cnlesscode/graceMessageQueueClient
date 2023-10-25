package client

import (
	"errors"
	"net"
	"strings"
	"time"
)

// 连接对象结构体
type TCPConnection struct {
	// TCP 连接指针
	Conn net.Conn
	// TCP 服务地址
	ServerAddr string
	// TCP 服务索引
	ServerIndex int
	// 真正使用的 TCP 服务地址
	CurrentServerAddr string
	// 真正使用的 TCP 服务索引
	CurrentServerIndex int
}

// 连接池结构体
type GraceTCPConnectionPool struct {
	// 默认 TCP 服务地址
	Addresses []string
	// TCP 服务地址数量
	AddressesLen int
	// 连接管道
	Channel chan *TCPConnection
	// 管道容量
	Capacity int
}

var ServerStatus = make(map[string]bool)

// 初始化连接池
// 参数 : TCP 服务 addr , 多个使用逗号分隔
func Init(addr string, capacity int) *GraceTCPConnectionPool {

	// 整理服务地址
	addrs := strings.Split(addr, ",")
	TCPConnectionAddrs := make([]string, 0)
	for _, addr := range addrs {
		addr = strings.ReplaceAll(addr, " ", "")
		TCPConnectionAddrs = append(
			TCPConnectionAddrs,
			addr,
		)
	}

	// 初始化连接池结构体
	tcpConnectionPool := &GraceTCPConnectionPool{
		Addresses:    TCPConnectionAddrs,
		AddressesLen: len(addrs),
		Channel:      make(chan *TCPConnection, capacity),
		Capacity:     capacity,
	}

	// 立即检查服务可用性
	canUseAddressesLen := tcpConnectionPool.checkAddrsIsAvailable()
	if canUseAddressesLen < tcpConnectionPool.AddressesLen {
		panic("TCP 服务不可用")
	}

	// 循环检查TCP服务
	go func() {
		for {
			time.Sleep(time.Second * 3)
			tcpConnectionPool.checkAddrsIsAvailable()
		}
	}()

	// 初始化填充连接池
	connectedNumber := 0
fillTip:
	// 循环填充连接
	for index := range tcpConnectionPool.Addresses {
		tcpConnection, _ := tcpConnectionPool.GetAnAvailableConn(index)
		tcpConnectionPool.Channel <- &tcpConnection
		connectedNumber += 1
		if connectedNumber >= tcpConnectionPool.Capacity {
			break
		}
	}
	if connectedNumber < tcpConnectionPool.Capacity {
		goto fillTip
	}

	// 返回连接池
	return tcpConnectionPool
}

// 获取一个可用的连接
func (st *GraceTCPConnectionPool) GetAnAvailableConn(index int) (TCPConnection, error) {
	// 当前服务可用
	conn, err := net.DialTimeout("tcp", st.Addresses[index], time.Second*5)
	tcpConnection := TCPConnection{
		ServerAddr:         st.Addresses[index],
		ServerIndex:        index,
		CurrentServerAddr:  st.Addresses[index],
		CurrentServerIndex: index,
		Conn:               conn,
	}
	if err == nil {
		return tcpConnection, nil
	}
	// 当前服务不可用，找到可用的服务
	// 非集群模式
	if st.AddressesLen < 1 {
		return tcpConnection, errors.New("TCP 服务不可用")
	}
	// 集群模式继续寻找
	return st.FinAnAvailableConn(index)
}

// 查询一个可用的服务
func (st *GraceTCPConnectionPool) FinAnAvailableConn(index int) (TCPConnection, error) {
	tcpConnection := TCPConnection{
		ServerAddr:         st.Addresses[index],
		ServerIndex:        index,
		CurrentServerAddr:  st.Addresses[index],
		CurrentServerIndex: index,
	}
	nextIndex := index + 1
	var err error
	var conn net.Conn
	for {
		if nextIndex >= st.AddressesLen {
			nextIndex = 0
		}
		// 尝试连接
		conn, err = net.DialTimeout("tcp", st.Addresses[nextIndex], time.Second*5)
		if err == nil {
			tcpConnection.CurrentServerAddr = st.Addresses[nextIndex]
			tcpConnection.CurrentServerIndex = nextIndex
			tcpConnection.Conn = conn
			break
		}
		if index == nextIndex {
			break
		}
		nextIndex += 1
	}
	return tcpConnection, err
}

// 检查服务可用性
func (st *GraceTCPConnectionPool) checkAddrsIsAvailable() int {
	canUseAddressesLen := 0
	for _, addr := range st.Addresses {
		conn, err := net.DialTimeout("tcp", addr, time.Second*5)
		if err == nil {
			canUseAddressesLen++
			conn.Close()
			ServerStatus[addr] = true
		} else {
			ServerStatus[addr] = false
		}
	}
	return canUseAddressesLen
}

// 修复连接
func (st *GraceTCPConnectionPool) CorrectConnection(tcpConnection *TCPConnection) {
	if tcpConnection.CurrentServerIndex == tcpConnection.ServerIndex {
		return
	}
	status, ok := ServerStatus[tcpConnection.ServerAddr]
	if !ok || !status {
		return
	}
	conn, err := net.DialTimeout("tcp", tcpConnection.ServerAddr, time.Second*5)
	if err == nil {
		tcpConnection.Conn.Close()
		tcpConnection.Conn = conn
		tcpConnection.CurrentServerIndex = tcpConnection.ServerIndex
		tcpConnection.CurrentServerAddr = tcpConnection.ServerAddr
	}
}
