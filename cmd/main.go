package main

import (
	"log"

	cask "github.com/srivastavcodes/caskdb"
)

func main() {
	db, err := cask.Open(cask.DefaultOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()
}
