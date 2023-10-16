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

// 全局变量
var addr string = "192.168.1.100:3001"
var TCPConnetionPool *client.TCPConnectionPool
var err error

var wg sync.WaitGroup

func main() {
	// 观察协程
	go func() {
		for {
			fmt.Printf("协程数 : %v\n", runtime.NumGoroutine())
			time.Sleep(time.Second * 3)
		}
	}()
	// 初始化连接池
	TCPConnetionPool, err = client.InitTCPConnetionPool(addr, 2000)
	if err == nil {
		println("TCP 连接池初始化成功")
	} else {
		panic(err.Error())
	}
	for i := 1; i <= 200000; i++ {
		wg.Add(1)
		go func(step int) {
			defer wg.Done()
			// 生产消息
			message := client.Message{
				Type:  1,
				Topic: "topic1",
				Data:  strconv.Itoa(step) + "message data",
			}
			messageByte, _ := json.Marshal(message)
			err := TCPConnetionPool.SendMessage(messageByte, nil)
			if err != nil {
				fmt.Printf("err: %v\n", err)
			}
		}(i)
	}
	wg.Wait()
	for {
		time.Sleep(time.Hour)
	}
	println("--- main done ---")
}
