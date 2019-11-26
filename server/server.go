package server

import "fmt"

// StartServer starts the HTTPS server on a port with a given keyfile path
func StartServer(port int, pathToKeyfile string) {
	fmt.Printf("Starting server on port %v\n", port)
}
