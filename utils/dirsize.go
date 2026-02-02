package utils

import (
	"os"
	"path/filepath"
)

// DirSize walks the directory structure and computes the size of every file
// even if subdirectories are found. It returns the size and an error if any.
func DirSize(dirPath string) (int64, error) {
	var size int64
	walkFn := func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			size += info.Size()
		}
		return nil
	}
	err := filepath.WalkDir(dirPath, walkFn)
	return size, err
}
