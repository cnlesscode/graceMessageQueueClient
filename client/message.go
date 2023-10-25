package client

import (
	"encoding/json"
	"net"
	"strings"
	"time"
)

// 发送消息结构体
type Message struct {
	Type          int
	Topic         string
	ConsumerGroup string
	Data          any
}

// 响应消息结构体
type ResponseMessage struct {
	Errcode int    `json:"errcode"`
	Data    string `json:"data"`
}

// 1 查看话题
func (st *GraceTCPConnectionPool) ListTopics() ResponseMessage {
	// 查询话题列表
	message := Message{
		Type: 4,
	}
	return st.SendAndReceive(message)
}

// 2 创建话题
func (st *GraceTCPConnectionPool) CreateTopic(topic string) ResponseMessage {
	successedRes := make([]string, 0)
	filedRes := make([]string, 0)
	responseMessage := ResponseMessage{
		Errcode: 0,
		Data:    "",
	}
	// 集群创建话题
	for _, addr := range st.Addresses {
		conn, err := net.DialTimeout("tcp", addr, time.Second*5)
		if err != nil {
			filedRes = append(filedRes, addr+" 连接失败")
			continue
		} else {
			// 查询话题列表
			message := Message{
				Type:  3,
				Topic: topic,
			}
			messageByte, _ := json.Marshal(message)
			// 写消息
			_, err := conn.Write(messageByte)
			if err != nil {
				filedRes = append(filedRes, addr+" 请求失败")
				continue
			}
			// 等待响应
			// 监听服务端响应
			buf := make([]byte, 5120)
			n, err := conn.Read(buf)
			if err != nil {
				filedRes = append(filedRes, addr+" 创建话题失败")
				continue
			}
			// 执行成功
			conn.Close()
			// 解析消息
			buf = buf[0:n]
			err = json.Unmarshal(buf, &responseMessage)
			if err == nil {
				successedRes = append(successedRes, addr+" "+responseMessage.Data)
			} else {
				responseMessage.Errcode = 400440
				filedRes = append(filedRes, addr+" 创建话题失败")
			}
		}
	}
	responseMessage.Data = strings.Join(successedRes, ",") + "\n" + strings.Join(filedRes, ",")
	return responseMessage
}

// 生产消息
func (st *GraceTCPConnectionPool) ProductMessage(topic string, data any) ResponseMessage {
	message := Message{
		Type:  1,
		Topic: topic,
		Data:  data,
	}
	return st.SendAndReceive(message)

}

// 生产消息
func (st *GraceTCPConnectionPool) ConsumeMessage(topic, consumerGroup string) ResponseMessage {
	message := Message{
		Type:          2,
		Topic:         topic,
		ConsumerGroup: consumerGroup,
	}
	return st.SendAndReceive(message)
}

// 发送消息基础函数
func (st *GraceTCPConnectionPool) SendAndReceive(message any) ResponseMessage {
	responseMessage := ResponseMessage{
		Errcode: 0,
		Data:    "",
	}
	messageByte, err := json.Marshal(message)
	if err != nil {
		responseMessage.Errcode = 500500
		responseMessage.Data = "消息数据错误"
		return responseMessage
	}
	// 从通道获取一个连接
	tcpConnectionST := <-st.Channel
	// 最终给回连接池
	defer func() {
		st.Channel <- tcpConnectionST
	}()
	doNumber := 1
WriteMsgTip:
	// 写消息
	_, err = tcpConnectionST.Conn.Write(messageByte)
	if err != nil {
		// 写入失败 尝试重连
		if doNumber < 2 {
			doNumber++
			tcpConnectionSTNew, err := st.GetAnAvailableConn(tcpConnectionST.CurrentServerIndex)
			if err == nil {
				tcpConnectionST = &tcpConnectionSTNew
				goto WriteMsgTip
			}
		}
		responseMessage.Errcode = 400410
		responseMessage.Data = "消息发送失败"
		return responseMessage
	}

	// 监听服务端响应
	buf := make([]byte, 3072)
	n, err := tcpConnectionST.Conn.Read(buf)

	// err != nil 代表服务端断开
	if err != nil {
		responseMessage.Errcode = 400430
		responseMessage.Data = "服务端已断开"
		return responseMessage
	}
	buf = buf[0:n]
	err = json.Unmarshal(buf, &responseMessage)
	// 交互成功
	if err == nil {
		// 修正连接
		if st.AddressesLen > 1 {
			st.CorrectConnection(tcpConnectionST)
		}
		return responseMessage
	}
	responseMessage.Errcode = 400440
	responseMessage.Data = "响应消息格式错误"
	return responseMessage
}
