package buffer

import (
	"fmt"
	"time"

	"github.com/immrshc/simple-db-go/file"
	"github.com/immrshc/simple-db-go/log"
)

const maxWaitDuration = time.Second * 10

type Manager struct {
	pool         []*Buffer
	availableNum int
	unpinChan    chan struct{}
}

func NewManager(fm *file.Manager, lm *log.Manager, num int) *Manager {
	pool := make([]*Buffer, num)
	for i := 0; i < num; i++ {
		pool[i] = NewBuffer(fm, lm)
	}
	return &Manager{
		pool:         pool,
		availableNum: num,
		unpinChan:    make(chan struct{}, num),
	}
}

func (m *Manager) FlushAll(txnNum int) error {
	for _, buff := range m.pool {
		if buff.TxnNum() != txnNum {
			continue
		}
		if err := buff.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) Unpin(buff *Buffer) error {
	buff.Unpin()
	if buff.Pinned() {
		return nil
	}
	m.availableNum++
	m.unpinChan <- struct{}{}
	return nil
}

func (m *Manager) Pin(blk *file.BlockID) (*Buffer, error) {
	buff, err := m.tryToPin(blk)
	if err != nil {
		return nil, err
	}
	if buff == nil {
		select {
		case <-m.unpinChan:
		case <-time.Tick(maxWaitDuration):
		}
		if buff, err = m.tryToPin(blk); err != nil {
			return nil, err
		}
	}
	if buff != nil {
		return buff, nil
	}
	return nil, fmt.Errorf("blockID(%s) not buffered", blk.String())
}

func (m *Manager) tryToPin(blk *file.BlockID) (*Buffer, error) {
	buff := m.findExistingBuffer(blk)
	if buff == nil {
		buff = m.chooseUnpinnedBuffer()
		if buff == nil {
			return nil, nil
		}
		if err := buff.AssignToBlock(blk); err != nil {
			return nil, err
		}
	}
	if !buff.Pinned() {
		m.availableNum--
	}
	buff.Pin()
	return buff, nil
}

func (m *Manager) findExistingBuffer(blk *file.BlockID) *Buffer {
	for _, buff := range m.pool {
		if buff.Block().Equal(blk) {
			return buff
		}
	}
	return nil
}

func (m *Manager) chooseUnpinnedBuffer() *Buffer {
	for _, buff := range m.pool {
		if !buff.Pinned() {
			return buff
		}
	}
	return nil
}
