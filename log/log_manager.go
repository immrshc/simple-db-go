package log

import (
	"sync"

	"github.com/immrshc/simple-db-go/file"
)

type Manager struct {
	mu           sync.Mutex
	fm           *file.Manager
	fileName     string
	logPage      *file.Page
	currentBlock *file.BlockID
	latestLSN    int
	lastSavedLSN int
}

func NewManager(fm *file.Manager, fileName string) (*Manager, error) {
	lm := &Manager{
		fm:       fm,
		fileName: fileName,
	}
	page := file.NewPage(fm.BlockSize())
	size, err := fm.CountBlocks(fileName)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		lm.currentBlock, err = lm.appendBlock()
		if err != nil {
			return nil, err
		}
	} else {
		lm.currentBlock = file.NewBlockID(fileName, size)
		if err = fm.WriteBlock(lm.currentBlock, page); err != nil {
			return nil, err
		}
	}
	return lm, nil
}

func (m *Manager) Flush(lsn int) error {
	if lsn < m.lastSavedLSN {
		return nil
	}
	return m.flush()
}

func (m *Manager) flush() error {
	m.lastSavedLSN = m.latestLSN
	return m.fm.WriteBlock(m.currentBlock, m.logPage)
}

func (m *Manager) AppendRecord(record []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// The place where the last record was written is appended into the head of a log page.
	// Records are written from left to right in a log page, and consists of the size and contents.
	boundary, err := m.logPage.ReadInt(0)
	if err != nil {
		return 0, err
	}
	size := int64(len(record))
	required := size + file.PageIntSize
	if boundary-required < file.PageIntSize {
		if err = m.flush(); err != nil {
			return 0, err
		}
		if m.currentBlock, err = m.appendBlock(); err != nil {
			return 0, err
		}
		boundary, err = m.logPage.ReadInt(0)
		if err != nil {
			return 0, err
		}
	}
	pos := boundary - required
	if err = m.logPage.WriteBytes(pos, record); err != nil {
		return 0, err
	}
	if err = m.logPage.WriteInt(0, pos); err != nil {
		return 0, err
	}
	m.latestLSN++
	return m.latestLSN, nil
}

func (m *Manager) appendBlock() (*file.BlockID, error) {
	blk, err := m.fm.AppendBlock(m.fileName)
	if err != nil {
		return nil, err
	}
	if err = m.logPage.WriteInt(0, m.fm.BlockSize()); err != nil {
		return nil, err
	}
	if err = m.fm.WriteBlock(blk, m.logPage); err != nil {
		return nil, err
	}
	return blk, nil
}
