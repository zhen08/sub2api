package callaudit

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

type BodyEncoding int

const (
	BodyJSON BodyEncoding = iota
	BodyUTF8Raw
)

// WriteArtifactEnvelope appends a body field to a metadata object without
// loading the captured body into memory. BodyJSON preserves the legacy parsed
// request/JSON response shape; BodyUTF8Raw writes {raw,encoding} for SSE/text.
func WriteArtifactEnvelope(writer io.Writer, fields map[string]any, body io.Reader, bodyEncoding BodyEncoding) error {
	metadata, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	if len(metadata) == 0 || metadata[len(metadata)-1] != '}' {
		return fmt.Errorf("audit artifact metadata must be an object")
	}
	if _, err := writer.Write(metadata[:len(metadata)-1]); err != nil {
		return err
	}
	if _, err := io.WriteString(writer, `,"body":`); err != nil {
		return err
	}
	if body == nil {
		_, err = io.WriteString(writer, "null}")
		return err
	}
	switch bodyEncoding {
	case BodyJSON:
		if _, err := io.Copy(writer, body); err != nil {
			return err
		}
	case BodyUTF8Raw:
		if _, err := io.WriteString(writer, `{"raw":"`); err != nil {
			return err
		}
		if err := writeEscapedUTF8(writer, body); err != nil {
			return err
		}
		if _, err := io.WriteString(writer, `","encoding":"utf8"}`); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported audit body encoding")
	}
	_, err = io.WriteString(writer, "}")
	return err
}

func writeEscapedUTF8(writer io.Writer, reader io.Reader) error {
	buffered := bufio.NewReader(reader)
	for {
		r, size, err := buffered.ReadRune()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if r == utf8.RuneError && size == 1 {
			r = '\uFFFD'
		}
		var escaped string
		switch r {
		case '"':
			escaped = `\"`
		case '\\':
			escaped = `\\`
		case '\b':
			escaped = `\b`
		case '\f':
			escaped = `\f`
		case '\n':
			escaped = `\n`
		case '\r':
			escaped = `\r`
		case '\t':
			escaped = `\t`
		default:
			if r < 0x20 {
				escaped = fmt.Sprintf(`\u%04x`, r)
			} else {
				escaped = string(r)
			}
		}
		if _, err := io.Copy(writer, strings.NewReader(escaped)); err != nil {
			return err
		}
	}
}
