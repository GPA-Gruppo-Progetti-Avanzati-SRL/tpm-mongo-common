package keystring

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

func NewBufferFromString(k string) (Buffer, error) {
	b, err := hex.DecodeString(k)
	if err != nil {
		return Buffer{}, err
	}

	return Buffer{_buf: b}, nil
}

type Buffer struct {
	_buf  []byte
	index int
}

func (buf *Buffer) bytesAvailable(numBytes int) bool {
	return len(buf._buf) > buf.index+numBytes
}

func (buf *Buffer) peekByte() (byte, bool) {
	if len(buf._buf) > buf.index {
		return buf._buf[buf.index], true
	}

	return 0, false
}

func (buf *Buffer) readByte() (byte, error) {
	if len(buf._buf) > buf.index {
		v := buf._buf[buf.index]
		buf.index++
		return v, nil
	}

	return 0, fmt.Errorf("buffer too small")
}

func (buf *Buffer) readBytes(numBytes int) ([]byte, error) {
	if len(buf._buf) > buf.index+numBytes {
		arr := buf._buf[buf.index : buf.index+numBytes]
		buf.index += numBytes
		return arr, nil
	}

	return nil, fmt.Errorf("buffer too small")
}

func (buf *Buffer) readUint32BE() (uint32, error) {

	arr, err := buf.readBytes(4)
	if err != nil {
		return 0, err
	}

	v := uint32(arr[0])<<24 + uint32(arr[1])<<16 + uint32(arr[2])<<8 + uint32(arr[3])
	return v, nil
}

func (buf *Buffer) readUint64BE() (uint64, error) {
	v1, err := buf.readUint32BE()
	if err != nil {
		return 0, err
	}

	v2, err := buf.readUint32BE()
	if err != nil {
		return 0, err
	}

	return uint64(v1)<<32 + uint64(v2), nil
}

func (buf *Buffer) readCString() (string, error) {

	end := bytes.IndexByte(buf._buf[buf.index:], 0)
	if end == -1 {
		end = len(buf._buf)
	}

	str := string(buf._buf[buf.index : buf.index+end])
	buf.index += end
	_, _ = buf.readByte()
	return str, nil
}

func (buf *Buffer) readCStringWithNulls() (string, error) {
	str, err := buf.readCString()
	if err != nil {
		return "", err
	}

	b, ok := buf.peekByte()
	for ok && b == 0xff {
		b, _ = buf.readByte()
		s1, err := buf.readCString()
		if err != nil {
			return "", err
		}
		str += "\\0" + s1
		b, ok = buf.peekByte()
	}

	return str, nil
}
