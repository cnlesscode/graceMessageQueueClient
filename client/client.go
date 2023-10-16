package client

// 从管道内获取一个连接
// 长时间获取不到则创建一个
func (st *TCPConnectionPool) GetAConnFromChannel() *TCPConnection {
	tcpConnection := <-st.Channel
	return tcpConnection
}
