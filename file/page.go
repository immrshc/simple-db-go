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

type Page struct {
	buf []byte
	pos int
}

func NewPage(blockSize int) *Page {
	return &Page{
		buf: make([]byte, blockSize),
		pos: 0,
	}

}

func NewPageFromBytes(b []byte) *Page {
	return &Page{
		buf: b,
		pos: 0,
	}
}

func (p *Page) position(pos int) error {
	if len(p.buf) <= pos || pos < 0 {
		return ErrInvalidPosition
	}
	p.pos = pos
	return nil
}

func (p *Page) Contents() *Page {
	p.pos = 0
	return p
}

func (p *Page) Read(data []byte) (int, error) {
	if len(p.buf[p.pos:]) > len(data) {
		return 0, ErrShortBuffer
	}
	n := copy(data, p.buf[p.pos:])
	p.pos += n
	return n, nil
}

func (p *Page) ReadInt(offset int) (int32, error) {
	if err := p.position(offset); err != nil {
		return 0, err
	}
	// Int32 consists of 4 bytes.
	b := make([]byte, 4)
	n, err := p.Read(b)
	if err != nil {
		return 0, err
	}
	p.pos += n
	return int32(binary.LittleEndian.Uint32(b)), nil
}

func (p *Page) ReadBytes(offset int) ([]byte, error) {
	size, err := p.ReadInt(offset)
	if err != nil {
		return nil, err
	}
	b := make([]byte, size)
	n, err := p.Read(b)
	if err != nil {
		return nil, err
	}
	p.pos += n
	return b, nil
}

func (p *Page) ReadString(offset int) (string, error) {
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
	p.pos += n
	return n, nil
}

func (p *Page) WriteInt(offset int, n int32) error {
	if err := p.position(offset); err != nil {
		return err
	}
	return binary.Write(p, binary.LittleEndian, n)
}

func (p *Page) WriteBytes(offset int, b []byte) error {
	if err := p.WriteInt(offset, int32(len(b))); err != nil {
		return err
	}
	_, err := p.Write(b)
	return err
}

func (p *Page) WriteString(offset int, s string) error {
	return p.WriteBytes(offset, []byte(s))
}
