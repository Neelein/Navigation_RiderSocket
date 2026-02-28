package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
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

	jsondata, err := json.Marshal(map[string]string{
		"Topic":     "test",
		"Partition": "0"})

	if err != nil {
		log.Fatal("json marshal error", err)
	}

	defer conn.Close()

	for {
		messageType, data, err := conn.ReadMessage()

		if err != nil {
			log.Fatal(err)
			return
		}

		fmt.Println(data)

		resp, err := http.Post(
			"http://localhost:5000/api/v2/write",
			"*/*",
			bytes.NewBuffer(jsondata),
		)

		bodyByte, err := io.ReadAll(resp.Body)

		if err != nil {
			log.Fatal("read resp fail", err)
		}

		fmt.Println(string(bodyByte))

		if err := conn.WriteMessage(messageType, data); err != nil {
			log.Fatal(err)
			return
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
