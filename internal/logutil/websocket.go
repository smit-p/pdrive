package logutil

import (
	"crypto/sha1" //nolint:gosec // SHA-1 required by WebSocket RFC 6455
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
)

const websocketMagic = "258EAFA5-E914-47DA-95CA-5AB9B3F6C588"

// upgradeWebSocket performs the HTTP → WebSocket upgrade handshake (RFC 6455)
// and returns the raw TCP connection. This avoids a gorilla/websocket dependency.
func upgradeWebSocket(w http.ResponseWriter, r *http.Request) (net.Conn, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return nil, errors.New("missing Upgrade: websocket header")
	}

	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, errors.New("missing Sec-WebSocket-Key header")
	}

	// Compute accept key per RFC 6455 §4.2.2.
	h := sha1.New() //nolint:gosec
	h.Write([]byte(key + websocketMagic))
	accept := base64.StdEncoding.EncodeToString(h.Sum(nil))

	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("server does not support hijacking")
	}

	conn, bufrw, err := hj.Hijack()
	if err != nil {
		return nil, fmt.Errorf("hijack failed: %w", err)
	}

	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"

	if _, err := bufrw.WriteString(resp); err != nil {
		conn.Close()
		return nil, err
	}
	if err := bufrw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

// writeWebSocketText sends a text frame over a WebSocket connection.
func writeWebSocketText(conn net.Conn, data []byte) error {
	// Text frame: FIN=1, opcode=1.
	var frame []byte
	frame = append(frame, 0x81) // FIN + text opcode

	length := len(data)
	switch {
	case length <= 125:
		frame = append(frame, byte(length))
	case length <= 65535:
		frame = append(frame, 126)
		buf := make([]byte, 2)
		binary.BigEndian.PutUint16(buf, uint16(length))
		frame = append(frame, buf...)
	default:
		frame = append(frame, 127)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(length))
		frame = append(frame, buf...)
	}

	frame = append(frame, data...)
	_, err := conn.Write(frame)
	return err
}
