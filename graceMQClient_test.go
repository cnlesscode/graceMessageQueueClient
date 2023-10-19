package main

import (
	"fmt"
	"testing"

	"github.com/cnlesscode/graceMessageQueueClient/client"
)

// go test -v -run=TestTCPClient
func TestTCPClient(*testing.T) {
	tcpClient, err := client.New("192.168.1.102:8802")
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return
	}
	defer tcpClient.Close()

	// 普通消息
	res, err := tcpClient.SendAndReceive("hi ...", false)
	if err != nil {
		fmt.Printf("err: %v\n", err)
	}
	fmt.Printf("res: %v\n", string(res))

	// json 消息
	message := map[string]any{
		"name": "lesscode",
		"age":  18,
	}
	res, err = tcpClient.SendAndReceive(message, true)
	if err != nil {
		fmt.Printf("err: %v\n", err)
	}
	fmt.Printf("res: %v\n", string(res))
}
