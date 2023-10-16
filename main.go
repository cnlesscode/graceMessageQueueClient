package main

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cnlesscode/gotool/random"
	"gopkg.in/ini.v1"
)

var connections = make([]*net.TCPConn, 0)
var connsUsed = make([]*net.TCPConn, 0)
var appRoot string = ""
var separator string = string(os.PathSeparator)

func main() {
	// 分析命令
	args := os.Args
	argsLength := len(args)
	if argsLength < 2 {
		PrintHelp()
		return
	}
	println("")
	println("------ Welcome To Use Grace TCP Client  ------")
	// 初始化项目目录
	exePath, err := os.Executable()
	if err != nil {
		panic("项目目录初始化失败")
	}
	absFilePath, err := filepath.EvalSymlinks(filepath.Dir(exePath))
	if err != nil {
		panic("项目目录初始化失败")
	}
	appRoot = absFilePath + separator
	configFile := appRoot + "config.ini"
	iniFile, err := ini.Load(configFile)
	if err != nil {
		println("------ TCP 服务配置读取失败  ------")
		return
	}
	// 字符串类型
	tcpAddrString := iniFile.Section("").Key("tcp-addr").String()
	addrs := strings.Split(tcpAddrString, ",")
	// 连接 TCP 服务
	for _, addr := range addrs {
		conn, err := ConnectTCP(addr)
		if err != nil {
			println("------ TCP : " + addr + " 连接失败  ------")
			return
		}
		connections = append(connections, conn)
		defer conn.Close()
	}
	// 检查连接成功数量
	connectedSize := len(connections)
	command := args[1]
	commandArgs := args[2:]
	commandArgsLength := len(commandArgs)
	// 随机选择一个连接
	connForUse := connections[random.RangeIntRand(0, int64(connectedSize)-1)]
	// 1. 生产消息
	if command == "product" {
		if commandArgsLength < 2 {
			println("命令参数错误")
			return
		}
		message := Message{Type: 1, Topic: commandArgs[0], Data: commandArgs[1]}
		sendMessage(connForUse, message)
	} else if command == "consume" { // 2. 消费消息
		if commandArgsLength < 2 {
			println("命令参数错误")
			return
		}
		message := Message{Type: 2, Topic: commandArgs[0], ConsumerGroup: commandArgs[1]}
		sendMessage(connForUse, message)
	} else if command == "create-topic" { // 3. 创建话题
		if commandArgsLength < 1 {
			println("请输入话题名称")
			return
		}
		message := Message{Type: 3, Topic: commandArgs[0]}
		for _, conn := range connections {
			sendMessage(conn, message)
		}
	} else if command == "topics" { //4. 话题列表
		message := Message{Type: 4}
		for _, conn := range connections {
			sendMessage(conn, message)
		}
	} else if command == "status" { //5. 查看状态
		message := Message{Type: 5}
		for _, conn := range connections {
			sendMessage(conn, message)
		}
	} else if command == "create-consumer-group" {
		if commandArgsLength < 2 {
			println("命令参数错误")
			return
		}
		// 遍历集群
		message := Message{Type: 6, Topic: commandArgs[0], ConsumerGroup: commandArgs[1]}
		for _, conn := range connections {
			sendMessage(conn, message)
		}
	}

	// 监听消息
	for _, conn := range connsUsed {
		buf := make([]byte, 2048)
		n, _ := conn.Read(buf)
		buf = buf[0:n]
		println(conn.RemoteAddr().String() + " 响应结果 :")
		println(string(buf))
		println("------------------------------------------------")
		println("")
	}

}

// 发送消息结构体
type Message struct {
	Type          int
	Topic         string
	ConsumerGroup string
	Data          string
}

// 发送消息
func sendMessage(conn *net.TCPConn, message Message) error {
	messageByte, err := json.Marshal(message)
	if err != nil {
		return err
	}
	_, err = conn.Write(messageByte)
	if err != nil {
		println("命令执行失败，错误 :")
		println(err.Error())
	}
	connsUsed = append(connsUsed, conn)
	return nil
}

// 打印 help
func PrintHelp() {
	println("")
	println("- 命令提示 :")
	println("  功能       命令            参数")
	println("- 查看话题   topics")
	println("例: ./TCPClient.exe topics")
	println("- 创建话题   create-topic   话题名称")
	println("例: ./TCPClient.exe create-topic topic1")
	println("- 生产消息   product        话题名称 消息内容")
	println("例: ./TCPClient.exe product topic1 hello...")
	println("- 消费话题   consume        话题名称 消费者组 消费条目数 ")
	println("例: ./TCPClient.exe consume topic1 default")
	println("- 创建消费者组   create-consumer-group 话题名称 消费者组")
	println("例: ./TCPClient.exe create-consumer-group topic1 consumer_group_1")
	println("- 查看服务   status")
	println("例: ./TCPClient.exe status")
}

// 连接 TCP 服务
func ConnectTCP(tcpAddrString string) (*net.TCPConn, error) {
	conn, err := net.DialTimeout("tcp", tcpAddrString, time.Second*3)
	if err != nil {
		return nil, err
	}
	connTcp, ok := conn.(*net.TCPConn)
	if ok {
		return connTcp, nil
	}
	return nil, errors.New("fail")
}
