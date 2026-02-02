package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetTestKey(t *testing.T) {
	for i := 0; i < 10; i++ {
		require.NotNil(t, GetTestKey(i))
	}
}

func TestRandomBytes(t *testing.T) {
	for i := 0; i < 10; i++ {
		require.NotNil(t, RandomBytes(i))
	}
}
