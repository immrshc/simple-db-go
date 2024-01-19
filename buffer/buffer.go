package buffer

import (
	"github.com/immrshc/simple-db-go/file"
	"github.com/immrshc/simple-db-go/log"
)

type Buffer struct {
	contents *file.Page
	blockID  *file.BlockID
	fm       *file.Manager
	lm       *log.Manager
	pins     int
	txNum    int
	lsn      int
}

func NewBuffer(fm *file.Manager, lm *log.Manager) *Buffer {
	return &Buffer{
		lm:       lm,
		fm:       fm,
		contents: file.NewPage(fm.BlockSize()),
	}
}

func (b *Buffer) TxnNum() int {
	return b.txNum
}

func (b *Buffer) SetModified(txnNum int, lsn int) {
	b.txNum = txnNum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) AssignToBlock(blockID *file.BlockID) error {
	if err := b.Flush(); err != nil {
		return err
	}
	b.blockID = blockID
	if err := b.fm.ReadBlock(b.blockID, b.contents); err != nil {
		return err
	}
	b.pins = 0
	return nil
}

func (b *Buffer) Flush() error {
	if b.txNum < 0 {
		return nil
	}
	if err := b.lm.Flush(b.lsn); err != nil {
		return err
	}
	if err := b.fm.WriteBlock(b.blockID, b.contents); err != nil {
		return err
	}
	b.txNum = -1
	return nil
}

func (b *Buffer) Pinned() bool {
	return b.pins > 0
}

func (b *Buffer) Pin() {
	b.pins++
}

func (b *Buffer) Unpin() {
	b.pins--
}
