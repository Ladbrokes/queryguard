package main

import (
	"flag"
	"log"
	"strings"
	"time"
)

func main() {
	messageTimeout := flag.Duration("message_timeout", 2*time.Minute, "timeout for one message to be proxied")
	clientIdleTimeout := flag.Duration("client_idle_timeout", 60*time.Minute, "idle timeout for client connections")
	addrs := flag.String("addrs", "localhost:27017", "comma separated list of mongo addresses")
	listen := flag.String("listen", ":6000", "port to listen for connections on")

	servers := []string{}
	for _, server := range strings.Split(*addrs, ",") {
		servers = append(servers, strings.TrimSpace(server))
	}

	log.Println("Listening on", *listen)
	log.Println("Forwarding to", strings.Join(servers, ", "))

	if err := newProxy(servers, *messageTimeout, *clientIdleTimeout).ListenAndRelay("tcp", *listen); err != nil {
		panic(err)
	}
}
