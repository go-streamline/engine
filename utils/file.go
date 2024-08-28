package utils

import (
	"io"
	"os"
)

var CopyFile = copyFile
var CreateDirsIfNotExist = createDirsIfNotExist

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return err
	}

	return nil
}

func OpenFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func createDirsIfNotExist(path ...string) error {
	for _, p := range path {
		if _, err := os.Stat(p); os.IsNotExist(err) {
			err := os.MkdirAll(p, os.ModePerm)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
