package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func writeHeartbeat(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(time.Now().Unix(), 10)+"\n"), 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("publish heartbeat: %w", err)
	}
	return nil
}

func heartbeatLoop(ctx context.Context, path string) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		_ = writeHeartbeat(path)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
