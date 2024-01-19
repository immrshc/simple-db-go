package log

import "github.com/immrshc/simple-db-go/file"

type Iterator struct {
	fm      *file.Manager
	blockID *file.BlockID
	page    *file.Page
	pos     int64
	record  []byte
}

func NewIterator(fm *file.Manager, blk *file.BlockID) (*Iterator, error) {
	i := &Iterator{
		fm:      fm,
		blockID: blk,
		page:    file.NewPage(fm.BlockSize()),
	}
	err := i.moveToBlock(i.blockID)
	return i, err
}

func (i *Iterator) moveToBlock(blk *file.BlockID) error {
	if err := i.fm.ReadBlock(blk, i.page); err != nil {
		return err
	}
	boundary, err := i.page.ReadInt(0)
	if err != nil {
		return err
	}
	i.pos = boundary
	return nil
}

func (i *Iterator) Next() bool {
	if i.pos >= i.fm.BlockSize() && i.blockID.Number() <= 0 {
		return false
	}
	if i.pos == i.fm.BlockSize() {
		blk := file.NewBlockID(i.blockID.FileName(), i.blockID.Number()-1)
		if err := i.moveToBlock(blk); err != nil {
			return false
		}
	}
	record, err := i.page.ReadBytes(i.pos)
	if err != nil {
		return false
	}
	i.pos += int64(8 + len(record))
	i.record = record
	return true
}

func (i *Iterator) Record() []byte {
	return i.record
}
