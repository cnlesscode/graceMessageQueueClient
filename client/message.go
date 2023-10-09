package client

// 发送消息结构体
type Message struct {
	Type          int
	Topic         string
	ConsumerGroup string
	Data          string
}
