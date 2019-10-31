package main

import (
	"fmt"

	"github.com/notduncansmith/loghive/loghive"
)

func main() {
	fmt.Printf("%v\n", loghive.NewLog("main", []byte("'allo")))
}
