package tcp

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

// Codec handles message encoding/decoding.
type Codec struct {
	maxMessageSize int
}

// NewCodec creates a new codec.
func NewCodec(maxMessageSize int) *Codec {
	return &Codec{
		maxMessageSize: maxMessageSize,
	}
}

// EncodeMessage marshals any protobuf message with a length prefix.
func (c *Codec) EncodeMessage(msg proto.Message) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	if len(data) > c.maxMessageSize {
		return nil, fmt.Errorf("message size %d exceeds max %d", len(data), c.maxMessageSize)
	}

	// Format: [4 bytes length][protobuf data]
	result := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(result[:4], uint32(len(data))) //nolint: gosec // Safe
	copy(result[4:], data)

	return result, nil
}

// DecodeMessage reads a length-prefixed protobuf message.
func (c *Codec) DecodeMessage(r io.Reader, msg proto.Message) error {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return err
	}

	length := binary.BigEndian.Uint32(lengthBuf)
	if int(length) > c.maxMessageSize {
		return fmt.Errorf("message size %d exceeds max %d", length, c.maxMessageSize)
	}

	if length == 0 {
		return fmt.Errorf("empty message")
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}

	return proto.Unmarshal(data, msg)
}

// WriteMessage writes a message to a writer.
func (c *Codec) WriteMessage(w io.Writer, msg proto.Message) error {
	data, err := c.EncodeMessage(msg)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	return err
}

// ReadMessage reads a message from a reader.
func (c *Codec) ReadMessage(r *bufio.Reader, msg proto.Message) error {
	return c.DecodeMessage(r, msg)
}
