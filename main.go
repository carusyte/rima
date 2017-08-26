package main

import (
	"github.com/chrislusf/gleam/gio"
	"net"
	"net/http"
	"net/rpc"
	"log"
	"github.com/carusyte/rima/rsec"
	"github.com/carusyte/rima/tes"
)

func main() {
	gio.Init() // If the command line invokes the mapper or reducer, execute it and exit.

	//Register RPC services
	rpc.Register(new(rsec.IndcScorer))
	rpc.Register(new(rsec.DataSync))
	rpc.Register(new(tes.GTest))

	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":45321")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Printf("listening on port: %d", 45321)
	http.Serve(l, nil)
}
