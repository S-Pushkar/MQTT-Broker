package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

type ClientData struct {
	ClientType string
	Topic      string
	Message    string
}

func main() {
	conn, err := net.Dial("tcp", "localhost:1883")
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	var producer_consumer int32

	fmt.Print("Type 1 to be a producer, 2 to be a consumer, or any other number to exit: ")
	fmt.Scanln(&producer_consumer)
	reader := bufio.NewReader(os.Stdin)

	if producer_consumer == 1 {
		// Producer
		for {
			fmt.Print("Enter the topic you want to publish to: ")
			var topic string
			fmt.Scanln(&topic)
			fmt.Print("Enter the message you want to publish: ")
    		message, err := reader.ReadString('\n')
    		if err != nil {
    		    fmt.Println("Error:", err)
    		    return
    		}
    		message = strings.TrimSpace(message)

			data := &ClientData{ ClientType: "producer", Topic: topic, Message: message }

			marshaledData, err := json.Marshal(data)
			if err != nil {
				log.Fatalf("Failed to marshal data: %v\n", err)
				return
			}
			_, err = conn.Write(marshaledData)

			if err != nil {
				log.Printf("Failed to write data: %v\n", err)
				return
			}
		}
	} else if producer_consumer == 2 {
		// Consumer
		fmt.Print("Please enter the topic you want to subscribe to: ")
		var topic string
		fmt.Scanln(&topic)

		data := &ClientData{ ClientType: "consumer", Topic: topic, Message: "" }
		marshaledData, err := json.Marshal(data)

		if err != nil {
			log.Fatalf("Failed to marshal data: %v\n", err)
			return
		}

		_, err = conn.Write(marshaledData)

		if err != nil {
			log.Printf("Failed to write data: %v\n", err)
			return
		}

		for {
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)

			if err != nil {
				log.Printf("Failed to read from connection: %v\n", err)
				return
			}

			fmt.Printf("Received message from broker: %s\n", buf[:n])
		}
	} else {
		return
	}
}