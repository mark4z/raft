package main

import (
	"os"
	"strconv"
	"strings"
)

func main() {
	localPort, _ := strconv.Atoi(os.Getenv("localPort"))
	peers := strings.Split(os.Getenv("peers"), ",")
	var peersList []int
	for _, p := range peers {
		port, _ := strconv.Atoi(p)
		peersList = append(peersList, port)
	}
	NewCore(localPort, peersList).Start()
}
