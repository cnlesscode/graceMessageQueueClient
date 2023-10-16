package client

import (
	"errors"
)

// 发送消息结构体
type Message struct {
	Type          int
	Topic         string
	ConsumerGroup string
	Data          string
}

// 发送接收消息
// 收到响应后执行的函数
type ResponseFunc func(data []byte) error

// 发送消息
func (st *TCPConnectionPool) SendMessage(message []byte, responseFunc ResponseFunc) error {
	doNumber := 1
SendMessageTip:
	tcpConnectionST := <-st.Channel
	// fmt.Printf(
	// 	"ServerAddr:%v CurrentServerAddr:%v Status:%v CurrentServerIndex:%v ServerIndex:%v\n",
	// 	tcpConnectionST.ServerAddr,
	// 	tcpConnectionST.CurrentServerAddr,
	// 	tcpConnectionST.Status,
	// 	tcpConnectionST.CurrentServerIndex,
	// 	tcpConnectionST.ServerIndex,
	// )
	// 检查连接有效性
	// 当前连接无效
	if tcpConnectionST.Conn == nil || !tcpConnectionST.Status {
		// 无效的连接放回连接池
		// 系统会自动重连
		if tcpConnectionST.ServerIndex != -1 {
			st.Channel <- tcpConnectionST
		}
		if doNumber < st.AddressesLen+1 {
			// 重新取一个连接
			doNumber++
			goto SendMessageTip
		} else {
			return errors.New("消息发送失败 : 服务端连接失败")
		}
	}

	// 写消息
	_, err := tcpConnectionST.Conn.Write(message)
	if err != nil {
		// 写入失败
		// 如果连接来自连接池，更新连接状态
		if tcpConnectionST.ServerIndex != -1 {
			tcpConnectionST.Status = false
			// 无效的连接放回连接池
			// 系统会自动重连
			st.Channel <- tcpConnectionST
		}
		if doNumber < st.AddressesLen+1 {
			doNumber++
			goto SendMessageTip
		}
		return err
	}

	// 监听服务端响应
	buf := make([]byte, 3072)
	n, err := tcpConnectionST.Conn.Read(buf)
	// err != nil 代表服务端断开
	if err != nil {
		// 如果连接来自连接池，更新连接状态
		if tcpConnectionST.ServerIndex != -1 {
			tcpConnectionST.Status = false
			// 无效的连接放回连接池
			// 系统会自动重连
			st.Channel <- tcpConnectionST
		}
		if doNumber < st.AddressesLen+1 {
			doNumber++
			goto SendMessageTip
		}
		return err
	}

	// 执行成功
	st.Channel <- tcpConnectionST
	buf = buf[0:n]

	// 是否需要等待应答
	if responseFunc == nil {
		return nil
	}

	return responseFunc(buf)

}
