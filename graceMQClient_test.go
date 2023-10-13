package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cnlesscode/graceMessageQueueClient/client"
)

// go test -v -run=TestInit
func TestInit(t *testing.T) {
	tcpConnectionPool, err := client.InitTCPConnetionPool("192.168.1.102:3001, 192.168.1.100:3001", 1000)
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		println("初始化成功")
	}
	// 并发
	var wg sync.WaitGroup
	for i := 0; i <= 500000; i++ {
		wg.Add(1)
		go func() {
			// 发送消息
			message := client.Message{
				Type: 4,
			}
			messageByte, _ := json.Marshal(message)
			err = tcpConnectionPool.SendMessage(messageByte, func(data []byte) error {
				//print(".")
				return nil
			})
			if err != nil {
				fmt.Printf("err: %v\n", err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	println("")
	time.Sleep(time.Second * 3)
}
