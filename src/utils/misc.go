package utils

import (
	"io"
	"math/rand"
	"os"
	"path/filepath"
)

func ReadFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}

	return string(content), nil
}

func WriteFile(filename string, content []byte) error {
	dir := filepath.Dir(filename)
	base := filepath.Base(filename)
	file, err := os.CreateTemp(dir, base)

	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(content)
	if err != nil {
		return err
	}

	err = os.Rename(file.Name(), filename)
	if err != nil {
		return err
	}

	return nil
}

func Random1024() int64 {
	return int64(rand.Intn(1024))
}
