package utils

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirSize(t *testing.T) {
	dir, _ := os.Getwd()
	dirSize, err := DirSize(dir)

	fmt.Printf("dir: %s, dirSize: %d\n", dir, dirSize)

	require.NoError(t, err)
	require.Positive(t, dirSize)
}
