package file

type BlockID struct {
	fileName string
	blockNum int64 // It corresponds to how many blocks are used.
}

func NewBlockID(name string, num int64) *BlockID {
	return &BlockID{
		fileName: name,
		blockNum: num,
	}
}

func (bid *BlockID) FileName() string {
	return bid.fileName
}

func (bid *BlockID) Number() int64 {
	return bid.blockNum
}
