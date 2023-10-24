package main

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/cnlesscode/graceMessageQueueClient/client"
)

// 全局变量
var addr string = "192.168.1.102:8881"
var graceMQConnetionPool *client.GraceTCPConnectionPool
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
	graceMQConnetionPool = client.Init(addr, 2000)
	for i := 1; i <= 50000; i++ {
		wg.Add(1)
		go func(step int) {
			defer wg.Done()
			// 生产消息
			res := graceMQConnetionPool.ProductMessage("topic1", "Messsge ["+strconv.Itoa(step)+"]")
			if res.Errcode != 0 {
				fmt.Printf("res.Data: %v\n", res.Data)
			}
		}(i)
	}
	wg.Wait()
	println("--- main done ---")
	for {
		time.Sleep(time.Hour)
	}
}
