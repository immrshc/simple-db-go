package recovery

import (
	"fmt"

	"github.com/immrshc/simple-db-go/log"

	"github.com/immrshc/simple-db-go/file"
	"github.com/immrshc/simple-db-go/tx"
)

const (
	Checkpoint int64 = iota
	Start
	Commit
	Rollback
	SetInt
	SetString
)

type LogRecord interface {
	// Operation returns the log record's type.
	Operation() int64
	// TxNumber returns the transaction id stored with the log record.
	TxNumber() int64
	// Undo restores a record to the previous one, according to the log record.
	// The only log record types involved into this operation are SetInt and SetString.
	Undo(tx *tx.Transaction) error
}

func CreateLogRecord(record []byte) (LogRecord, error) {
	page := file.WrapInPage(record)
	lo, err := page.ReadInt(0)
	if err != nil {
		return nil, err
	}
	switch lo {
	case Checkpoint:
		return nil, nil
	case Start:
		return nil, nil
	case Commit:
		return nil, nil
	case Rollback:
		return nil, nil
	case SetInt:
		return nil, nil
	case SetString:
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid log type: %d", lo)
	}
}

func WriteStartRecord(lm *log.Manager, txn int64) error {
	page := file.NewPage(file.PageIntSize * 2)
	if err := page.WriteInt(0, Start); err != nil {
		return err
	}
	if err := page.WriteInt(file.PageIntSize, txn); err != nil {
		return err
	}
	_, err := lm.AppendRecord(page.Contents())
	return err
}

func WriteSetStringRecord(lm *log.Manager, txn int64, blk *file.BlockID, offset int64, val string) error {
	return nil
}

func WriteSetIntRecord(lm *log.Manager, txn int64, blk *file.BlockID, offset, val int64) error {
	return nil
}

func WriteRollbackRecord(lm *log.Manager, txn int64) error {
	page := file.NewPage(file.PageIntSize * 2)
	if err := page.WriteInt(0, Rollback); err != nil {
		return err
	}
	if err := page.WriteInt(file.PageIntSize, txn); err != nil {
		return err
	}
	_, err := lm.AppendRecord(page.Contents())
	return err
}

func WriteCommitRecord(lm *log.Manager, txn int64) error {
	page := file.NewPage(file.PageIntSize * 2)
	if err := page.WriteInt(0, Commit); err != nil {
		return err
	}
	if err := page.WriteInt(file.PageIntSize, txn); err != nil {
		return err
	}
	_, err := lm.AppendRecord(page.Contents())
	return err
}

func WriteCheckpointRecord(lm *log.Manager) error {
	page := file.NewPage(file.PageIntSize)
	if err := page.WriteInt(0, Checkpoint); err != nil {
		return err
	}
	_, err := lm.AppendRecord(page.Contents())
	return err
}
