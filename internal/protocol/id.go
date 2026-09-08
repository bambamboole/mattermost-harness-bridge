package protocol

import (
	"crypto/rand"
	"strings"

	"github.com/oklog/ulid/v2"
)

// NewID returns a prefixed ULID such as "job_01J7...". Prefixes make log
// lines and database rows readable without a schema in your head.
func NewID(prefix string) string {
	return prefix + "_" + ulid.MustNew(ulid.Now(), rand.Reader).String()
}

// HasPrefix reports whether id was made by NewID with the given prefix.
func HasPrefix(id, prefix string) bool {
	return strings.HasPrefix(id, prefix+"_") && len(id) == len(prefix)+1+ulid.EncodedSize
}
