package utils

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	mu      sync.Mutex
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GetTestKey returns a formatted key to be used for internal tests.
func GetTestKey(idx int) []byte {
	return fmt.Appendf(nil, "caskdb-test-key-%09d", idx)
}

// RandomBytes generates a sequence of random bytes for internal tests.
func RandomBytes(length int) []byte {
	bytes := make([]byte, length)

	for i := range bytes {
		mu.Lock()
		bytes[i] = letters[randStr.Intn(len(letters))]
		mu.Unlock()
	}
	return bytes
}
