package storage

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

const manifestName = "MANIFEST"

const maxLevels = 7

// levels[0] lists L0 tables oldest first; deeper levels list tables in key order.
type manifest struct {
	logNum uint64
	levels [][]uint64
}

func (m manifest) tables() []uint64 {
	return slices.Concat(m.levels...)
}

func (m manifest) encode() []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "log %d\n", m.logNum)
	for level, nums := range m.levels {
		fmt.Fprintf(&b, "level %d", level)
		for _, num := range nums {
			fmt.Fprintf(&b, " %d", num)
		}
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, "crc %d\n", crc32.Checksum(b.Bytes(), crcTable))
	return b.Bytes()
}

func writeManifest(dir string, m manifest) error {
	return writeFileAtomic(filepath.Join(dir, manifestName), m.encode())
}

func readManifest(dir string) (m manifest, found bool, err error) {
	data, err := os.ReadFile(filepath.Join(dir, manifestName))
	if errors.Is(err, os.ErrNotExist) {
		return manifest{}, false, nil
	}
	if err != nil {
		return manifest{}, false, err
	}
	m, err = parseManifest(data)
	if err != nil {
		return manifest{}, false, fmt.Errorf("%s: %w", manifestName, err)
	}
	return m, true, nil
}

func parseManifest(data []byte) (manifest, error) {
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) < 2 {
		return manifest{}, ErrCorrupt
	}
	last := len(lines) - 1
	body := strings.Join(lines[:last], "\n") + "\n"
	var sum uint32
	if _, err := fmt.Sscanf(lines[last], "crc %d", &sum); err != nil || sum != crc32.Checksum([]byte(body), crcTable) {
		return manifest{}, ErrCorrupt
	}

	var m manifest
	if _, err := fmt.Sscanf(lines[0], "log %d", &m.logNum); err != nil {
		return manifest{}, ErrCorrupt
	}
	for _, line := range lines[1:last] {
		fields := strings.Fields(line)
		level := 0
		switch {
		case len(fields) >= 1 && fields[0] == "tables":
			fields = fields[1:]
		case len(fields) >= 2 && fields[0] == "level":
			n, err := strconv.Atoi(fields[1])
			if err != nil || n < 0 || n >= maxLevels {
				return manifest{}, ErrCorrupt
			}
			level, fields = n, fields[2:]
		default:
			return manifest{}, ErrCorrupt
		}
		for len(m.levels) <= level {
			m.levels = append(m.levels, nil)
		}
		for _, f := range fields {
			num, err := strconv.ParseUint(f, 10, 64)
			if err != nil {
				return manifest{}, ErrCorrupt
			}
			m.levels[level] = append(m.levels[level], num)
		}
	}
	return m, nil
}
