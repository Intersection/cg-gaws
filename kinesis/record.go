package kinesis

import (
	"encoding/base64"
)

// Bytes decodes the data in a record and returns it as []byte
func (r *Record) Bytes() ([]byte, error) {
	result, err := base64.StdEncoding.DecodeString(r.Data)
	return result, err
}
