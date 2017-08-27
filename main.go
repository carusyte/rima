package main

import (
	"github.com/chrislusf/gleam/gio"
	"net"
	"net/rpc"
	"log"
	"github.com/carusyte/rima/rsec"
	"github.com/carusyte/rima/tes"
)

func main() {
	gio.Init() // If the command line invokes the mapper or reducer, execute it and exit.

	svr := rpc.NewServer()
	//Register RPC services
	svr.Register(new(rsec.IndcScorer))
	svr.Register(new(rsec.DataSync))
	svr.Register(new(tes.GTest))

	l, e := net.Listen("tcp", ":45321")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Printf("listening on port: %d", 45321)

	// This statement links rpc server to the socket, and allows rpc server to accept
	// rpc request coming from that socket.
	svr.Accept(l)
}
