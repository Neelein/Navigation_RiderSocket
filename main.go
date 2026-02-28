package main

import (
	"log"
)

func main() {
	server := NewServer()
	log.Fatal(server.Start())
}
