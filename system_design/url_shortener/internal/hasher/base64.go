package hasher

import "encoding/base64"

type Base64Hasher struct {
}

func (h *Base64Hasher) Encode(val string) string {
	return base64.StdEncoding.EncodeToString([]byte(val))
}

// TODO: return error, need to update interface
func (h *Base64Hasher) Decode(val string) string {
	enc, _ := base64.StdEncoding.DecodeString(val)
	return string(enc)
}
