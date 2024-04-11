package main

import (
	"encoding/json"
	"log"
	"net"
	"time"
)

type ClientData struct {
	ClientType string
	Topic      string
	Message    string
}

type Client struct {
	Conn       net.Conn
	TopicIndex int32 // index -> indices for Topics array
	// Mutex      sync.Mutex
}

type Broker struct {
	Topics      map[string][]string  // map[topic] = [data1, data2, ...]
	Subscribers map[string][]*Client // map[topic] = [client1, client2, ...]
	// Mutex       sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		Topics:      make(map[string][]string),
		Subscribers: make(map[string][]*Client),
	}
}

func (b *Broker) Run() {
	listener, err := net.Listen("tcp", ":1883")
	if err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}

	defer listener.Close()
	log.Println("MQTT broker started, listening on :1883")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		go b.handleConnection(conn)
	}
}

func (b *Broker) handleProducer(conn net.Conn, Topic string, Message string) {
	if _, ok := b.Topics[Topic]; !ok {
		b.Topics[Topic] = make([]string, 0)
	}

	b.Topics[Topic] = append(b.Topics[Topic], Message)

	if _, ok := b.Subscribers[Topic]; !ok {
		b.Subscribers[Topic] = make([]*Client, 0)
	}

	for _, client := range b.Subscribers[Topic] {
		for i := client.TopicIndex; i < int32(len(b.Topics[Topic])); i++ {
			client.Conn.Write([]byte(b.Topics[Topic][i]))
			time.Sleep(100 * time.Microsecond)
		}

		client.TopicIndex = int32(len(b.Topics[Topic]))
	}
}

func (b *Broker) handleConsumer(conn net.Conn, Topic string) {
	if _, ok := b.Topics[Topic]; !ok {
		b.Topics[Topic] = make([]string, 0)
	}

	if _, ok := b.Subscribers[Topic]; !ok {
		b.Subscribers[Topic] = make([]*Client, 0)
	}

	client := &Client{
		Conn:       conn,
		TopicIndex: 0,
	}

	b.Subscribers[Topic] = append(b.Subscribers[Topic], client)

	for i := client.TopicIndex; i < int32(len(b.Topics[Topic])); i++ {
		client.Conn.Write([]byte(b.Topics[Topic][i]))
		time.Sleep(100 * time.Microsecond)
	}

	client.TopicIndex = int32(len(b.Topics[Topic]))
}

func (b *Broker) handleConnection(conn net.Conn) {
	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Printf("Failed to read from connection: %v", err)
			return
		}

		var data ClientData
		err = json.Unmarshal(buf[:n], &data)

		if err != nil {
			log.Printf("Failed to unmarshal data: %v", err)
			return
		}

		if data.ClientType == "producer" {
			b.handleProducer(conn, data.Topic, data.Message)
		} else if data.ClientType == "consumer" {
			conn.Write([]byte("Connection Established! Subscribed to broker."))
			b.handleConsumer(conn, data.Topic)
		}
	}
}

func main() {
	broker := NewBroker()
	broker.Run()
}