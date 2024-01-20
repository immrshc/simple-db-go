package recovery

import "github.com/immrshc/simple-db-go/tx"

type CheckpointRecord struct{}

func (cr *CheckpointRecord) NewCheckpointRecord() *CheckpointRecord {
	return &CheckpointRecord{}
}
func (cr *CheckpointRecord) Operation() int64 {
	return Checkpoint
}
func (cr *CheckpointRecord) TxnNumber() int {
	return -1
}
func (cr *CheckpointRecord) Undo(tx *tx.Transaction) error {
	return nil
}
