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
func (st *GraceMQConnectionPool) ListTopics() ResponseMessage {
	// 查询话题列表
	message := Message{
		Type: 4,
	}
	messageByte, _ := json.Marshal(message)
	return st.SendMessageBase(messageByte)
}

// 2 创建话题
func (st *GraceMQConnectionPool) CreateTopic(topic string) ResponseMessage {
	successedRes := make([]string, 0)
	filedRes := make([]string, 0)
	responseMessage := ResponseMessage{
		Errcode: 0,
		Data:    "",
	}
	// 集群创建话题
	for _, addr := range st.Addresses {
		conn, err := net.DialTimeout("tcp", addr.Addr, time.Second*5)
		if err != nil {
			filedRes = append(filedRes, addr.Addr+" 连接失败")
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
				filedRes = append(filedRes, addr.Addr+" 请求失败")
				continue
			}
			// 等待响应
			// 监听服务端响应
			buf := make([]byte, 3072)
			n, err := conn.Read(buf)
			if err != nil {
				filedRes = append(filedRes, addr.Addr+" 创建话题失败")
				continue
			}
			// 执行成功
			conn.Close()
			// 解析消息
			buf = buf[0:n]
			err = json.Unmarshal(buf, &responseMessage)
			if err == nil {
				successedRes = append(successedRes, addr.Addr+" "+responseMessage.Data)
			} else {
				responseMessage.Errcode = 400440
				filedRes = append(filedRes, addr.Addr+" 创建话题失败")
			}
		}
	}
	responseMessage.Data = strings.Join(successedRes, ",") + "\n" + strings.Join(filedRes, ",")
	return responseMessage
}

// 生产消息
func (st *GraceMQConnectionPool) ProductMessage(topic string, data any) ResponseMessage {
	message := Message{
		Type:  1,
		Topic: topic,
		Data:  data,
	}
	messageByte, _ := json.Marshal(message)
	return st.SendMessageBase(messageByte)

}

// 生产消息
func (st *GraceMQConnectionPool) ConsumeMessage(topic string) ResponseMessage {
	message := Message{
		Type:  2,
		Topic: topic,
	}
	messageByte, _ := json.Marshal(message)
	return st.SendMessageBase(messageByte)

}

// 发送消息基础函数
func (st *GraceMQConnectionPool) SendMessageBase(message []byte) ResponseMessage {
	responseMessage := ResponseMessage{
		Errcode: 0,
		Data:    "",
	}
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
			responseMessage.Errcode = 400400
			responseMessage.Data = "消息发送失败 : 服务端连接失败"
			return responseMessage
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
		responseMessage.Errcode = 400410
		responseMessage.Data = "消息发送失败"
		return responseMessage
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
		responseMessage.Errcode = 400430
		responseMessage.Data = "服务端已断开"
		return responseMessage
	}

	// 执行成功
	st.Channel <- tcpConnectionST
	buf = buf[0:n]
	err = json.Unmarshal(buf, &responseMessage)
	if err == nil {
		return responseMessage
	}
	responseMessage.Errcode = 400440
	responseMessage.Data = "响应消息格式错误"
	return responseMessage
}
