package tx

import (
	"slices"

	"github.com/immrshc/simple-db-go/buffer"
	"github.com/immrshc/simple-db-go/file"
)

type BufferList struct {
	buffers map[*file.BlockID]*buffer.Buffer
	pins    []*file.BlockID
	bm      *buffer.Manager
}

func NewBufferList(bm *buffer.Manager) *BufferList {
	return &BufferList{
		bm: bm,
	}
}

func (bl *BufferList) Buffer(blk *file.BlockID) *buffer.Buffer {
	return bl.buffers[blk]
}

func (bl *BufferList) Pin(blk *file.BlockID) error {
	buff, err := bl.bm.Pin(blk)
	if err != nil {
		return err
	}
	bl.buffers[blk] = buff
	bl.pins = append(bl.pins, blk)
	return nil
}

func (bl *BufferList) Unpin(blk *file.BlockID) error {
	buff := bl.buffers[blk]
	if err := bl.bm.Unpin(buff); err != nil {
		return err
	}
	idx := slices.Index(bl.pins, blk)
	bl.pins = append(bl.pins[:idx], bl.pins[idx+1:]...)
	if !slices.Contains(bl.pins, blk) {
		delete(bl.buffers, blk)
	}
	return nil
}

func (bl *BufferList) UnpinAll() error {
	for _, blk := range bl.pins {
		buff := bl.buffers[blk]
		if err := bl.bm.Unpin(buff); err != nil {
			return err
		}
	}
	bl.buffers = map[*file.BlockID]*buffer.Buffer{}
	bl.pins = make([]*file.BlockID, 0)
	return nil
}
