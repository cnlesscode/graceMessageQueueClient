package client

import (
	"errors"
	"net"
	"strings"
	"time"

	"github.com/cnlesscode/gotool/random"
)

// 连接对象结构体
type TCPConnection struct {
	Conn *net.TCPConn
	Addr string
}

// 连接池结构体
type TCPConnectionPool struct {
	Addresses  []string
	AddressLen int
	Channel    chan *TCPConnection
	Capacity   int
}

// 收到响应后执行的函数
type ResponseFunc func(data []byte) error

// 发送消息
func (st *TCPConnectionPool) SendMessage(message []byte, responseFunc ResponseFunc) error {
	doNumber := 1
	TCPConnectionST := st.GetATCPConn()
	defer func() {
		st.Channel <- TCPConnectionST
	}()
SendMessageTip:
	_, err := TCPConnectionST.Conn.Write(message)
	// 发生错误
	if err != nil {
		if doNumber >= 2 {
			return err
		}
		doNumber += 1
		// 关闭错误的连接
		TCPConnectionST.Conn.Close()
		// 重新建立一个连接
		conn, addr, err := st.ConnectTCP()
		// 成功建立
		if err == nil {
			TCPConnectionST = &TCPConnection{Conn: conn, Addr: addr}
			goto SendMessageTip
		} else {
			// 建立失败
			return err
		}
	}
	buf := make([]byte, 3072)
	n, _ := TCPConnectionST.Conn.Read(buf)
	buf = buf[0:n]
	// 是否需要等待应答
	if responseFunc == nil {
		return nil
	}
	return responseFunc(buf)
}

// 接收消息
func (st *TCPConnectionPool) ReceiveMessage(conn *net.TCPConn, responseFunc ResponseFunc) error {
	TCPConnectionST := st.GetATCPConn()
	defer func() {
		st.Channel <- TCPConnectionST
	}()
	if responseFunc == nil {
		return nil
	}
	buf := make([]byte, 3072)
	n, _ := TCPConnectionST.Conn.Read(buf)
	buf = buf[0:n]
	return responseFunc(buf)
}

// 获取一个连接
func (st *TCPConnectionPool) GetATCPConn() *TCPConnection {
	return <-st.Channel
}

// 初始化连接池
// 参数 : TCP 服务 addr , 多个使用逗号分隔
func InitTCPConnetionPool(addr string, capacity int) *TCPConnectionPool {
	addrs := strings.Split(addr, ",")
	addrSize := len(addrs)
	tcpConnectionPool := &TCPConnectionPool{
		Addresses:  addrs,
		AddressLen: addrSize,
		Channel:    make(chan *TCPConnection, capacity),
		Capacity:   capacity,
	}
	// 初始化填充连接池
	successNumber := 0
	errNumder := 0
	for {
		conn, connAddr, err := tcpConnectionPool.ConnectTCP()
		if err == nil {
			successNumber += 1
			tCPConnection := &TCPConnection{Conn: conn, Addr: connAddr}
			tcpConnectionPool.Channel <- tCPConnection
			if successNumber >= capacity {
				break
			}
		} else {
			errNumder += 1
			if errNumder >= 5 {
				break
			}
		}
	}
	// 没有成功的连接或者错误率极高
	if errNumder >= 5 {
		panic("TCP连接池初始化失败 : 无法连接TCP服务器。")
	}
	return tcpConnectionPool
}

// 连接 TCP 服务
func (st *TCPConnectionPool) ConnectTCP() (*net.TCPConn, string, error) {
	addr := ""
	if st.AddressLen < 2 {
		addr = st.Addresses[0]
	} else {
		addr = st.Addresses[random.RangeIntRand(0, int64(st.AddressLen)-1)]
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second*5)
	if err != nil {
		return nil, addr, err
	}
	connTcp, ok := conn.(*net.TCPConn)
	if ok {
		return connTcp, addr, nil
	}
	return nil, addr, errors.New("fail")
}
