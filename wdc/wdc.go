package main

import (
	"flag"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/gio/mapper"
	"github.com/chrislusf/gleam/gio/reducer"
	"github.com/chrislusf/gleam/plugins/file"
	"log"
)

var (
	isDistributed   = flag.Bool("distributed", false, "run in distributed or not")
	isDockerCluster = flag.Bool("onDocker", false, "run in docker cluster")
)

func main() {
	log.Printf("WordCount called")
	flag.Parse() // optional, since gio.Init() will call this also.
	gio.Init()   // If the command line invokes the mapper or reducer, execute it and exit.
	//flag.Parse() // optional, since gio.Init() will call this also.
	f := flow.New("top5 words in yum").
		Read(file.Txt("/etc/yum.repos.d/CentOS-Base.repo", 2)). // read a txt file and partitioned to 2 shards
		Map("tokenize", mapper.Tokenize). // invoke the registered "tokenize" mapper function.
		Map("addOne", mapper.AppendOne).  // invoke the registered "addOne" mapper function.
		ReduceByKey("sum", reducer.Sum). // invoke the registered "sum" reducer function.
		Sort("sortBySum", flow.OrderBy(2, true)).
		Top("top5", 5, flow.OrderBy(2, false)).
		Printlnf("%s\t%d")
	f.Run(distributed.Option())
	log.Printf("WordCount finished")
}
