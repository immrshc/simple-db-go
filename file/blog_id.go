package file

type BlockID struct {
	fileName string
	blockNum int
}

func NewBlockID(name string, num int) *BlockID {
	return &BlockID{
		fileName: name,
		blockNum: num,
	}
}

func (bid *BlockID) FileName() string {
	return bid.fileName
}

func (bid *BlockID) Number() int {
	return bid.blockNum
}
