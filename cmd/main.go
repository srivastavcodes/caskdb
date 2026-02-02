package main

import (
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var randName = rand.NewSource(time.Now().UnixNano())

func main() {
	str := filepath.Join("./", "caskdb"+strconv.Itoa(int(randName.Int63())))
	err := os.MkdirAll(str, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
}
