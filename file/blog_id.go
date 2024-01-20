package file

import "fmt"

type BlockID struct {
	fileName string
	// blockNum corresponds to how many blocks are used.
	// This is used when seeking in os.File, which internally requires Int64 as an offset.
	blockNum int64
}

func NewBlockID(name string, num int64) *BlockID {
	return &BlockID{
		fileName: name,
		blockNum: num,
	}
}

func (b *BlockID) FileName() string {
	return b.fileName
}

func (b *BlockID) Number() int64 {
	return b.blockNum
}

func (b *BlockID) Equal(blk *BlockID) bool {
	return b.String() == blk.String()
}

func (b *BlockID) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.fileName, b.blockNum)
}
