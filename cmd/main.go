package main

import (
	"fmt"
	"log"
	"os"
	"time"

	cask "github.com/srivastavcodes/caskdb"
)

func main() {
	example3()
}

func example1() {
	db, err := cask.Open(cask.DefaultOptions)
	if err != nil {
		log.Fatal(err)
	}
	// defer closing the db gracefully and dir deletion
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(cask.DefaultOptions.DirPath)
	}()

	// set a key
	err = db.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		log.Fatal(err)
	}
	// get a key
	val, err := db.Get([]byte("key1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("value for key1: %s\n", val)

	// set the second key
	err = db.Put([]byte("key2"), []byte("value2"))
	if err != nil {
		log.Fatal(err)
	}
	// get the second key
	val, err = db.Get([]byte("key2"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("value for key2: %s\n", val)

	val, err = db.Get([]byte("key3"))
	if err != nil {
		log.Println("should err")
	}
	// replace for key1 and put a ttl with it.
	err = db.PutWithTTL([]byte("key1"), []byte("value1"), 3*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	val, err = db.Get([]byte("key1"))
}

func example2() {
	db, err := cask.Open(cask.DefaultOptions)
	if err != nil {
		log.Fatal(err)
	}
	// defer closing the db gracefully and dir deletion
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(cask.DefaultOptions.DirPath)
	}()

	// set a key
	err = db.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		log.Fatal(err)
	}
	// get a key
	val, err := db.Get([]byte("key1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("1st access for key1: %s\n", val)

	// replace for key1 and put a ttl with it.
	err = db.PutWithTTL([]byte("key1"), []byte("value1"), 3*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(3 * time.Second)

	val, err = db.Get([]byte("key1"))
	if err != nil {
		log.Printf("expired access for key1: %v\n", err)
	}
}

func example3() {
	db, err := cask.Open(cask.DefaultOptions)
	if err != nil {
		log.Fatal(err)
	}
	// defer closing the db gracefully and dir deletion
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(cask.DefaultOptions.DirPath)
	}()
	// create a batch with default options
	batch := db.NewBatch(cask.DefaultBatchOptions)

	err = batch.Put([]byte("key1"), []byte("value1"))
	err = batch.Put([]byte("key2"), []byte("value2"))
	err = batch.Put([]byte("key3"), []byte("value3"))

	if err != nil {
		log.Println(batch.Rollback())
	}

	// batch access before commit
	val, err := batch.Get([]byte("key1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("batch key1: %s\n", val)

	if err = batch.Commit(); err != nil {
		log.Println(err.Error())
	}
	// access after commit
	val, _ = batch.Get([]byte("key1"))
	fmt.Printf("batch key1: %s\n", val)
	val, _ = batch.Get([]byte("key2"))
	fmt.Printf("batch key2: %s\n", val)
	val, _ = batch.Get([]byte("key3"))
	fmt.Printf("batch key3: %s\n", val)
}
