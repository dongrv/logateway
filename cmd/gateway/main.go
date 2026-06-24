// Command gateway starts the logateway HTTP message gateway.
package main

import (
	"flag"
	"log"

	"github.com/dongrv/logateway/internal/server"
)

func main() {
	configPath := flag.String("config", "configs/gateway.yaml", "path to config file")
	flag.Parse()

	gw, err := server.New(*configPath)
	if err != nil {
		log.Fatalf("failed to start gateway: %v", err)
	}
	defer gw.Close()

	if err := gw.Run(); err != nil {
		log.Fatalf("gateway error: %v", err)
	}
}
