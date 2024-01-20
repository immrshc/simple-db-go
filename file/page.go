package file

import (
	"encoding/binary"
	"errors"
)

var (
	ErrInvalidPosition = errors.New("invalid buffer position")
	ErrShortWrite      = errors.New("short write")
	ErrShortBuffer     = errors.New("short buffer")
)

// PageIntSize represents the size of Int64, which consists of 8 bytes.
const PageIntSize int64 = 8

type Page struct {
	buf []byte
	pos int64
}

func NewPage(blockSize int64) *Page {
	return &Page{
		buf: make([]byte, blockSize),
		pos: 0,
	}
}

func WrapInPage(b []byte) *Page {
	return &Page{
		buf: b,
		pos: 0,
	}
}

func (p *Page) position(pos int64) error {
	if int64(len(p.buf)) <= pos || pos < 0 {
		return ErrInvalidPosition
	}
	p.pos = pos
	return nil
}

func (p *Page) Rewind() {
	p.pos = 0
}

func (p *Page) Contents() []byte {
	return p.buf
}

func (p *Page) Read(data []byte) (int, error) {
	if len(p.buf[p.pos:]) > len(data) {
		return 0, ErrShortBuffer
	}
	n := copy(data, p.buf[p.pos:])
	p.pos += int64(n)
	return n, nil
}

func (p *Page) ReadInt(offset int64) (int64, error) {
	if err := p.position(offset); err != nil {
		return 0, err
	}
	b := make([]byte, PageIntSize)
	n, err := p.Read(b)
	if err != nil {
		return 0, err
	}
	p.pos += int64(n)
	return int64(binary.LittleEndian.Uint64(b)), nil
}

func (p *Page) ReadBytes(offset int64) ([]byte, error) {
	size, err := p.ReadInt(offset)
	if err != nil {
		return nil, err
	}
	b := make([]byte, size)
	n, err := p.Read(b)
	if err != nil {
		return nil, err
	}
	p.pos += int64(n)
	return b, nil
}

func (p *Page) ReadString(offset int64) (string, error) {
	b, err := p.ReadBytes(offset)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (p *Page) Write(data []byte) (int, error) {
	if len(p.buf[p.pos:]) < len(data) {
		return 0, ErrShortWrite
	}
	n := copy(p.buf[p.pos:], data)
	p.pos += int64(n)
	return n, nil
}

// WriteInt writes a number into a page of which the position is specified by offset.
// The type of value inputted into a page should be fixed-size; int64 instead of int.
// Besides, an offset is basically calculated by a stored number in a page.
// So, the type of both offset and n is Int64.
func (p *Page) WriteInt(offset, n int64) error {
	if err := p.position(offset); err != nil {
		return err
	}
	return binary.Write(p, binary.LittleEndian, n)
}

func (p *Page) WriteBytes(offset int64, b []byte) error {
	if err := p.WriteInt(offset, int64(len(b))); err != nil {
		return err
	}
	_, err := p.Write(b)
	return err
}

func (p *Page) WriteString(offset int64, s string) error {
	return p.WriteBytes(offset, []byte(s))
}
