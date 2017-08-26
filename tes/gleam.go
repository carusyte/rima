package tes

import (
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/file"
	"github.com/chrislusf/gleam/distributed"
	"flag"
	"github.com/chrislusf/gleam/flow"
	"strings"
)

var (
	isDistributed = flag.Bool("distributed", false, "run in distributed or not")
	Tokenize      = gio.RegisterMapper(tokenize)
	AppendOne     = gio.RegisterMapper(appendOne)
	Sum           = gio.RegisterReducer(sum)
)

type GTest struct{}

func (d *GTest) WordCount(req *string, rep *bool) error {
	//gio.Init() // If the command line invokes the mapper or reducer, execute it and exit.
	sortOption := (&flow.SortOption{}).By(1, true)
	//flag.Parse() // optional, since gio.Init() will call this also.
	f := flow.New("top5 words in yum").
		Read(file.Txt("/etc/yum.repos.d/CentOS-Vault.repo", 2)). // read a txt file and partitioned to 2 shards
		Map("tokenize", Tokenize). // invoke the registered "tokenize" mapper function.
		Map("appendOne", AppendOne). // invoke the registered "appendOne" mapper function.
		ReduceBy("sum", Sum, sortOption). // invoke the registered "sum" reducer function.
		Sort("sortBySum", flow.OrderBy(2, true)).
		Top("top5", 5, flow.OrderBy(2, false)).
		Printlnf("%s\t%d")

	if *isDistributed {
		f.Run(distributed.Option())
	} else {
		f.Run()
	}
	return nil
}

func tokenize(row []interface{}) error {
	line := gio.ToString(row[0])
	for _, s := range strings.FieldsFunc(line, func(r rune) bool {
		return !('A' <= r && r <= 'Z' || 'a' <= r && r <= 'z' || '0' <= r && r <= '9')
	}) {
		gio.Emit(s)
	}
	return nil
}

func appendOne(row []interface{}) error {
	row = append(row, 1)
	gio.Emit(row...)
	return nil
}

func sum(x, y interface{}) (interface{}, error) {
	return gio.ToInt64(x) + gio.ToInt64(y), nil
}