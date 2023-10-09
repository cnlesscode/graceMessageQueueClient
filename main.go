package main

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/cnlesscode/graceMessageQueueClient/client"
)

// 初始化连接池
var addr string = "192.168.1.100:3001"
var TCPConnetionPool = client.InitTCPConnetionPool(addr, 300)

func main() {
	go func() {
		for {
			time.Sleep(time.Second * 3)
			fmt.Printf("连接数: %v\n", len(TCPConnetionPool.Channel))
			fmt.Printf("runtime.NumGoroutine(): %v\n", runtime.NumGoroutine())
		}
	}()

	// 创建话题
	//CreateTopic("topic1")

	// 生产消息
	ProductMessage()

	// 消费消息
	// ConsumeTopic("topic1")
}

// 消费话题
func ConsumeTopic(topic string) {
	for {
		message := client.Message{
			Type:  2,
			Topic: topic,
		}
		messageByte, _ := json.Marshal(message)
		err := TCPConnetionPool.SendMessage(messageByte, func(data []byte) error {
			fmt.Printf("data: %v\n", string(data))
			return nil
		})
		fmt.Printf("err: %v\n", err)
		if err != nil {
			return
		}
	}

}

// 创建话题
func CreateTopic(topic string) {
	message := client.Message{
		Type:  3,
		Topic: topic,
	}
	messageByte, _ := json.Marshal(message)
	err := TCPConnetionPool.SendMessage(messageByte, nil)
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		println("话题创建成功")
	}
}

// 生产消息示例
func ProductMessage() {
	// 多协程并行发送
	var wg sync.WaitGroup
	for ii := 1; ii <= 50000; ii++ {
		wg.Add(1)
		go func(stp int) {
			defer wg.Done()
			message := client.Message{
				Type:  1,
				Topic: "topic1",
				Data:  "[ " + strconv.Itoa(stp) + " ] 测试消息",
			}
			messageByte, _ := json.Marshal(message)
			err := TCPConnetionPool.SendMessage(messageByte, nil)
			if err != nil {
				fmt.Printf("err: %v\n", err)
			}
		}(ii)
	}
	wg.Wait()
	println("-------- 消息生产完毕 -------")
}
