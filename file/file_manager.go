package file

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Manager struct {
	mu        sync.Mutex
	dirPath   string
	blockSize int64
	files     map[string]*os.File
}

func NewManager(dirPath string, size int64) (*Manager, error) {
	_, err := os.Stat(dirPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if err = os.Mkdir(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "temp") {
			path := filepath.Join(dirPath, entry.Name())
			if err = os.Remove(path); err != nil {
				return nil, err
			}
		}
	}
	return &Manager{
		blockSize: size,
		dirPath:   dirPath,
	}, nil
}

func (m *Manager) BlockSize() int64 {
	return m.blockSize
}

func (m *Manager) Length(name string) (int64, error) {
	file, err := m.file(name)
	if err != nil {
		return 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	size := info.Size() / m.BlockSize()
	return size, nil
}

func (m *Manager) ReadBlock(blk BlockID, page *Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, err := m.file(blk.FileName())
	if err != nil {
		return err
	}
	if _, err = file.Seek(blk.Number()*m.BlockSize(), 0); err != nil {
		return err
	}
	page.Rewind()
	_, err = io.Copy(page, file)
	return err
}

func (m *Manager) WriteBlock(blk BlockID, page *Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, err := m.file(blk.FileName())
	if err != nil {
		return err
	}
	if _, err = file.Seek(blk.Number()*m.BlockSize(), 0); err != nil {
		return err
	}
	page.Rewind()
	_, err = io.Copy(file, page)
	return nil
}

func (m *Manager) AppendBlock(name string) (*BlockID, error) {
	blkNum, err := m.Length(name)
	if err != nil {
		return nil, err
	}
	blk := NewBlockID(name, blkNum)
	file, err := m.file(blk.FileName())
	if err != nil {
		return nil, err
	}
	if _, err = file.Seek(blk.Number()*m.BlockSize(), 0); err != nil {
		return nil, err
	}
	_, err = file.Write(make([]byte, m.blockSize))
	return blk, nil
}

func (m *Manager) file(name string) (*os.File, error) {
	file, ok := m.files[name]
	if ok {
		return file, nil
	}
	path := filepath.Join(m.dirPath, name)
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	m.files[name] = file
	return file, nil
}
