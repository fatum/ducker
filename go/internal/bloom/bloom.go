package bloom

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"math"
)

const minSize = 1024

func fnv1a(s string) uint32 {
	h := uint32(0x811c9dc5)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 0x01000193
	}
	return h
}

func murmurMix(s string) uint32 {
	h := uint32(0x12345678)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 0x5bd1e995
		h ^= h >> 15
	}
	return h
}

type Filter struct {
	Size      int
	HashCount int
	Bits      []byte
}

func NewFilter(size, hashCount int) *Filter {
	if size < 1 {
		size = minSize
	}
	if hashCount < 1 {
		hashCount = 1
	}
	return &Filter{Size: size, HashCount: hashCount, Bits: make([]byte, int(math.Ceil(float64(size)/8.0)))}
}

func (f *Filter) Add(value string) {
	h1 := fnv1a(value)
	h2 := murmurMix(value)
	for i := 0; i < f.HashCount; i++ {
		bit := int((h1 + uint32(i)*h2) % uint32(f.Size))
		f.Bits[bit>>3] |= 1 << (bit & 7)
	}
}

func (f *Filter) Probe(value string) bool {
	h1 := fnv1a(value)
	h2 := murmurMix(value)
	for i := 0; i < f.HashCount; i++ {
		bit := int((h1 + uint32(i)*h2) % uint32(f.Size))
		if f.Bits[bit>>3]&(1<<(bit&7)) == 0 {
			return false
		}
	}
	return true
}

type Serialized struct {
	Size      int    `json:"size"`
	HashCount int    `json:"hashCount"`
	Bits      string `json:"bits"`
}

type FileBloom struct {
	Columns map[string]Serialized `json:"columns"`
}

func Deserialize(s Serialized) (*Filter, error) {
	bits, err := base64.StdEncoding.DecodeString(s.Bits)
	if err != nil {
		return nil, err
	}
	return &Filter{Size: s.Size, HashCount: s.HashCount, Bits: bits}, nil
}

func ParseFileBloom(raw map[string]any) (FileBloom, error) {
	colsAny, ok := raw["columns"].(map[string]any)
	if !ok {
		return FileBloom{}, fmt.Errorf("missing columns")
	}
	cols := make(map[string]Serialized, len(colsAny))
	for col, v := range colsAny {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		s, okS := m["size"].(float64)
		h, okH := m["hashCount"].(float64)
		b, okB := m["bits"].(string)
		if !okS || !okH || !okB {
			continue
		}
		cols[col] = Serialized{Size: int(s), HashCount: int(h), Bits: b}
	}
	return FileBloom{Columns: cols}, nil
}

func ProbeFileBloom(b FileBloom, filters map[string]any) bool {
	for col, value := range filters {
		ser, ok := b.Columns[col]
		if !ok {
			continue
		}
		bf, err := Deserialize(ser)
		if err != nil {
			continue
		}
		switch v := value.(type) {
		case []string:
			anyPresent := false
			for _, item := range v {
				if bf.Probe(item) {
					anyPresent = true
					break
				}
			}
			if !anyPresent {
				return false
			}
		case []any:
			anyPresent := false
			for _, item := range v {
				if bf.Probe(fmt.Sprintf("%v", item)) {
					anyPresent = true
					break
				}
			}
			if !anyPresent {
				return false
			}
		default:
			if !bf.Probe(fmt.Sprintf("%v", v)) {
				return false
			}
		}
	}
	return true
}

func escapeSQL(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r == '\'' {
			out = append(out, '\'', '\'')
			continue
		}
		out = append(out, r)
	}
	return string(out)
}

func StoreToDB(conn *sql.DB, tenant, segment string, b FileBloom) error {
	for col, data := range b.Columns {
		bits, err := base64.StdEncoding.DecodeString(data.Bits)
		if err != nil {
			return err
		}
		_, err = conn.Exec(
			`INSERT OR REPLACE INTO _bloom_cache VALUES (?, ?, ?, ?, ?, ?)`,
			tenant, segment, col, data.Size, data.HashCount, bits,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func LoadFromDB(conn *sql.DB, tenant, segment string) (FileBloom, bool, error) {
	rows, err := conn.Query(
		`SELECT column_name, filter_size, hash_count, bits FROM _bloom_cache WHERE tenant = ? AND segment = ?`,
		tenant, segment,
	)
	if err != nil {
		return FileBloom{}, false, err
	}
	defer rows.Close()

	cols := map[string]Serialized{}
	for rows.Next() {
		var col string
		var size int
		var hashCount int
		var bits []byte
		if err := rows.Scan(&col, &size, &hashCount, &bits); err != nil {
			return FileBloom{}, false, err
		}
		cols[col] = Serialized{Size: size, HashCount: hashCount, Bits: base64.StdEncoding.EncodeToString(bits)}
	}
	if err := rows.Err(); err != nil {
		return FileBloom{}, false, err
	}
	if len(cols) == 0 {
		return FileBloom{}, false, nil
	}
	return FileBloom{Columns: cols}, true, nil
}
