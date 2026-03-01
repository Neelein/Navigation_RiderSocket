package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(h *http.Request) bool {
		return true
	},
}

type Server struct {
	Addr string
}

func NewServer() *Server {
	return &Server{
		Addr: ":5001",
	}
}

func handleRider(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		log.Fatal("connect server error", err)
	}

	if err != nil {
		log.Fatal("json marshal error", err)
	}

	defer conn.Close()

	for {
		messageType, _, err := conn.ReadMessage()

		if err != nil {
			log.Fatal("read message", err)
		}

		k := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{"localhost:3000", "localhost:3001"},
			Topic:     "test2",
			GroupID:   "customer-group",
			Partition: 0,
			MaxBytes:  10e6,
		})

		for {
			m, err := k.ReadMessage(context.Background())

			if err != nil {
				break
			}
			fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

			if err := conn.WriteMessage(messageType, []byte(m.Value)); err != nil {
				log.Fatal("write message err", err)
			}
		}

		if err := k.Close(); err != nil {
			log.Fatal("failed to close reader")
		}
	}
}

func (s *Server) Start() error {

	r := mux.NewRouter()
	route := r.PathPrefix("/").Subrouter()

	route.HandleFunc("/ws", handleRider)

	err := http.ListenAndServe(s.Addr, r)
	return err
}
