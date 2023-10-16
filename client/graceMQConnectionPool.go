package client

import (
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
	// 状态
	Status bool
}

// 连接服务地址结构体
type TCPConnectionAddr struct {
	Addr   string
	Status bool
}

// 连接池结构体
type GraceMQConnectionPool struct {
	// 默认 TCP 服务地址
	Addresses []*TCPConnectionAddr
	// TCP 服务地址数量
	AddressesLen int
	// 连接管道
	Channel chan *TCPConnection
	// 管道容量
	Capacity int
}

// 初始化连接池
// 参数 : TCP 服务 addr , 多个使用逗号分隔
func Init(addr string, capacity int) *GraceMQConnectionPool {

	// 整理服务地址
	addrs := strings.Split(addr, ",")
	TCPConnectionAddrs := make([]*TCPConnectionAddr, 0)
	for _, addr := range addrs {
		addr = strings.ReplaceAll(addr, " ", "")
		TCPConnectionAddrs = append(
			TCPConnectionAddrs,
			&TCPConnectionAddr{
				Addr:   addr,
				Status: false,
			},
		)
	}

	// 初始化连接池结构体
	tcpConnectionPool := &GraceMQConnectionPool{
		Addresses:    TCPConnectionAddrs,
		AddressesLen: len(addrs),
		Channel:      make(chan *TCPConnection, capacity),
		Capacity:     capacity,
	}

	// 立即检查服务可用性
	canUseAddressesLen := tcpConnectionPool.checkAddrsIsAvailable()
	if canUseAddressesLen < 1 {
		panic("TCP 服务不可用")
	}

	// 初始化填充连接池
	connectedNumber := 0
fillTip:
	// 循环填充连接
	for index := range tcpConnectionPool.Addresses {
		tcpConnection, _ := tcpConnectionPool.GetAnAvailableConn(index)
		tcpConnectionPool.Channel <- tcpConnection
		connectedNumber += 1
		if connectedNumber >= tcpConnectionPool.Capacity {
			break
		}
	}
	if connectedNumber < tcpConnectionPool.Capacity {
		goto fillTip
	}

	// 循环检查服务可用性
	go func() {
		for {
			// 此处需要延迟，否则会不断连接、断开 TCP 服务
			time.Sleep(time.Second * 5)
			tcpConnectionPool.checkAddrsIsAvailable()
		}
	}()

	// 检查连接池
	go func() {
		for {
			// 延迟 3 秒，避免过分抢占资源
			time.Sleep(time.Second * 3)
			// 检查闲置数量
			if (len(tcpConnectionPool.Channel)) < (tcpConnectionPool.Capacity / 2) {
				time.Sleep(time.Second * 3)
				continue
			}
			tcpConnection := <-tcpConnectionPool.Channel
			// 服务不可用
			if tcpConnection.Conn == nil || !tcpConnection.Status {
				// 获取一个新的连接
				tcpConnectionNew, err := tcpConnectionPool.GetAnAvailableConn(tcpConnection.ServerIndex)
				// 获取成功
				if err == nil {
					tcpConnection.Conn.Close()
					tcpConnectionPool.Channel <- tcpConnectionNew
				} else {
					// 获取失败
					tcpConnectionPool.Channel <- tcpConnection
				}
			} else {
				// 服务状态可用
				// 检查负载匹配，解决负载动态可用场景
				if tcpConnection.CurrentServerIndex != tcpConnection.ServerIndex {
					tcpConnectionNew, err := tcpConnectionPool.GetAnAvailableConn(tcpConnection.ServerIndex)
					if err == nil {
						tcpConnection.Conn.Close()
						tcpConnectionPool.Channel <- tcpConnectionNew
					} else {
						tcpConnectionPool.Channel <- tcpConnection
					}
				} else {
					// 返回给连接池
					tcpConnectionPool.Channel <- tcpConnection
				}
			}
		}
	}()

	// 返回连接池
	return tcpConnectionPool
}

// 获取一个可用的连接
func (st *GraceMQConnectionPool) GetAnAvailableConn(index int) (*TCPConnection, error) {
	// 当前服务可用
	if st.Addresses[index].Status {
		conn, err := net.DialTimeout("tcp", st.Addresses[index].Addr, time.Second*5)
		if err == nil {
			return &TCPConnection{
				ServerAddr:         st.Addresses[index].Addr,
				ServerIndex:        index,
				CurrentServerAddr:  st.Addresses[index].Addr,
				CurrentServerIndex: index,
				Conn:               conn,
				Status:             true,
			}, nil
		}
	}
	// 当前服务不可用，找到可用的服务
	return st.FinAnAvailableConn(index)
}

// 查询一个可用的服务
func (st *GraceMQConnectionPool) FinAnAvailableConn(index int) (*TCPConnection, error) {
	tcpConnection := &TCPConnection{
		ServerAddr:         st.Addresses[index].Addr,
		ServerIndex:        index,
		CurrentServerAddr:  st.Addresses[index].Addr,
		CurrentServerIndex: index,
		Status:             false,
	}
	nextIndex := index + 1
	var err error
	var conn net.Conn
	for {
		if nextIndex >= st.AddressesLen {
			nextIndex = 0
		}
		// 尝试连接
		conn, err = net.DialTimeout("tcp", st.Addresses[nextIndex].Addr, time.Second*5)
		if err == nil {
			tcpConnection.CurrentServerAddr = st.Addresses[nextIndex].Addr
			tcpConnection.CurrentServerIndex = nextIndex
			tcpConnection.Conn = conn
			tcpConnection.Status = true
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
func (st *GraceMQConnectionPool) checkAddrsIsAvailable() int {
	canUseAddressesLen := 0
	for _, addr := range st.Addresses {
		conn, err := net.DialTimeout("tcp", addr.Addr, time.Second*5)
		if err != nil {
			addr.Status = false
		} else {
			conn.Close()
			addr.Status = true
			canUseAddressesLen++
		}
	}
	return canUseAddressesLen
}
