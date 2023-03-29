package state

import (
	"encoding/binary"
	"io"
)

func (t *Command) Marshal(w io.Writer) {
	var b [8]byte

	// ClientId
	bs := b[:4]
	utmp32 := t.ClientId
	bs[0] = byte(utmp32)
	bs[1] = byte(utmp32 >> 8)
	bs[2] = byte(utmp32 >> 16)
	bs[3] = byte(utmp32 >> 24)
	w.Write(bs)

	// OpId
	bs = b[:4]
	tmp32 := t.OpId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	w.Write(bs)
	// Op
	bs = b[:1]
	b[0] = byte(t.Op)
	w.Write(bs)

	// K
	bs = b[:8]
	binary.LittleEndian.PutUint64(bs, uint64(t.K))
	w.Write(bs)

	// V
	// binary.LittleEndian.PutUint64(bs, uint64(t.V))
	vbytes := []byte(t.V)
	binary.LittleEndian.PutUint64(bs, uint64(len(vbytes))) // length first
	w.Write(bs)
	w.Write(vbytes) // then actual string bytes
}

func (t *Command) Unmarshal(r io.Reader) error {
	var b [8]byte
	bs := b[:4]

	// ClientId
	if _, err := io.ReadAtLeast(r, bs, 4); err != nil {
		return err
	}
	t.ClientId = uint32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	// OpId
	bs = b[:4]
	if _, err := io.ReadAtLeast(r, bs, 4); err != nil {
		return err
	}
	//t.OpId = OperationId((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.OpId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	// Op
	bs = b[:1]
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	t.Op = Operation(b[0])

	// K
	bs = b[:8]
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	t.K = Key(binary.LittleEndian.Uint64(bs))

	// V
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	vlen := binary.LittleEndian.Uint64(bs)
	vbs := make([]byte, vlen)
	if _, err := io.ReadFull(r, vbs); err != nil {
		return err
	}
	t.V = Value(vbs)

	return nil
}

func (t *Key) Marshal(w io.Writer) {
	var b [8]byte
	bs := b[:8]
	binary.LittleEndian.PutUint64(bs, uint64(*t))
	w.Write(bs)
}

func (t *Value) Marshal(w io.Writer) {
	var b [8]byte
	bs := b[:8]
	vbytes := []byte(*t)
	binary.LittleEndian.PutUint64(bs, uint64(len(vbytes)))
	w.Write(bs)
	w.Write(vbytes)
}

func (t *Key) Unmarshal(r io.Reader) error {
	var b [8]byte
	bs := b[:8]
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*t = Key(binary.LittleEndian.Uint64(bs))
	return nil
}

func (t *Value) Unmarshal(r io.Reader) error {
	var b [8]byte
	bs := b[:8]
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	vlen := binary.LittleEndian.Uint64(bs)
	vbs := make([]byte, vlen)
	if _, err := io.ReadFull(r, vbs); err != nil {
		return err
	}
	*t = Value(vbs)
	return nil
}
