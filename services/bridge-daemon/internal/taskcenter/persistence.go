package taskcenter

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const schemaVersion = 1

type corruptDataError struct{ err error }

func (e *corruptDataError) Error() string { return e.err.Error() }
func (e *corruptDataError) Unwrap() error { return e.err }

func preserveCorruptFile(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	stamp := time.Now().UTC().Format("20060102-150405.000000000")
	preserved := path + ".corrupt-" + stamp
	if err := copyFile(path, preserved); err == nil {
		return preserved
	}
	return ""
}

func copyFile(source, target string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	if copyErr == nil {
		copyErr = output.Sync()
	}
	closeErr := output.Close()
	if copyErr != nil {
		_ = os.Remove(target)
		return copyErr
	}
	return closeErr
}

func writeAtomic(path string, value any) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	writeErr := func() error {
		if _, err := file.Write(append(data, '\n')); err != nil {
			return err
		}
		return file.Sync()
	}()
	closeErr := file.Close()
	if writeErr != nil {
		_ = os.Remove(temporary)
		return writeErr
	}
	if closeErr != nil {
		_ = os.Remove(temporary)
		return closeErr
	}
	backup := path + ".bak"
	_ = os.Remove(backup)
	if _, err := os.Stat(path); err == nil {
		if err := os.Rename(path, backup); err != nil {
			_ = os.Remove(temporary)
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		_ = os.Remove(temporary)
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Rename(backup, path)
		_ = os.Remove(temporary)
		return err
	}
	_ = os.Remove(backup)
	return nil
}

func decodeFile(path string, target any, kind string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, target); err != nil {
		return &corruptDataError{err: fmt.Errorf("decode %s: %w", kind, err)}
	}
	return nil
}

func corruptWarning(path, kind string, err error) string {
	preserved := preserveCorruptFile(path)
	if preserved == "" {
		return fmt.Sprintf("%s 文件损坏，原文件无法自动复制保留：%v", kind, err)
	}
	return fmt.Sprintf("%s 文件损坏，已保留原文件并复制到 %s：%v", kind, preserved, err)
}
