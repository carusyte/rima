package main

import (
	"fmt"
	"log"
	"net/rpc"
	"testing"
)

const serverAddress = "127.0.0.1:45321"

func TestClient(t *testing.T) {
	client, err := rpc.DialHTTP("tcp", serverAddress)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// Synchronous call
	args := Args{7, 8}
	var reply int
	err = client.Call("Arith.Multiply", args, &reply)
	if err != nil {
		log.Fatal("arith error:", err)
	}
	fmt.Printf("Arith: %d*%d=%d\n", args.A, args.B, reply)

	// Asynchronous call
	quotient := new(Quotient)
	divCall := client.Go("Arith.Divide", args, quotient, nil)
	replyCall := <-divCall.Done // will be equal to divCall
	// check errors, print, etc.
	if replyCall.Error != nil {
		log.Fatal("arith error:", err)
	}
	q := replyCall.Reply.(*Quotient)
	fmt.Printf("Arith: %d/%d=%d, %d", args.A, args.B, q.Quo, q.Rem)
}
