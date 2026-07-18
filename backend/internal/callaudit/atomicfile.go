package callaudit

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func atomicWriteFile(filename string, data []byte, mode os.FileMode, exclusive bool) error {
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create audit spool directory: %w", err)
	}
	temp, err := os.CreateTemp(dir, ".callaudit-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary audit file: %w", err)
	}
	tempName := temp.Name()
	removeTemp := true
	defer func() {
		_ = temp.Close()
		if removeTemp {
			_ = os.Remove(tempName)
		}
	}()
	if err := temp.Chmod(mode); err != nil {
		return fmt.Errorf("set audit file permissions: %w", err)
	}
	if _, err := temp.Write(data); err != nil {
		return fmt.Errorf("write audit file: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync audit file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close audit file: %w", err)
	}
	if exclusive {
		if err := os.Link(tempName, filename); err != nil {
			if errors.Is(err, os.ErrExist) {
				return fmt.Errorf("audit file already exists: %w", err)
			}
			return fmt.Errorf("publish audit file: %w", err)
		}
		if err := os.Remove(tempName); err != nil {
			return fmt.Errorf("remove linked temporary audit file: %w", err)
		}
		removeTemp = false
	} else {
		if err := os.Rename(tempName, filename); err != nil {
			return fmt.Errorf("publish audit file: %w", err)
		}
		removeTemp = false
	}
	return syncDirectory(dir)
}

func syncDirectory(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}
