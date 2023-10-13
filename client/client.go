package client

import (
	"time"
)

// 从管道内获取一个连接
// 长时间获取不到则创建一个
func (st *TCPConnectionPool) GetAConnFromChannel() *TCPConnection {
	select {
	case tcpConnection := <-st.Channel:
		return tcpConnection
	case <-time.After(time.Second * 3):
		tcpConnection, _ := st.GetAnAvailableConn(0)
		tcpConnection.CurrentServerIndex = -1
		return tcpConnection
	}
}
