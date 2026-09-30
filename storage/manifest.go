package storage

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const manifestName = "MANIFEST"

type manifest struct {
	logNum uint64
	tables []uint64
}

func (m manifest) encode() []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "log %d\ntables", m.logNum)
	for _, num := range m.tables {
		fmt.Fprintf(&b, " %d", num)
	}
	b.WriteString("\n")
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
	if len(lines) != 3 {
		return manifest{}, ErrCorrupt
	}
	body := strings.Join(lines[:2], "\n") + "\n"
	var sum uint32
	if _, err := fmt.Sscanf(lines[2], "crc %d", &sum); err != nil || sum != crc32.Checksum([]byte(body), crcTable) {
		return manifest{}, ErrCorrupt
	}

	var m manifest
	if _, err := fmt.Sscanf(lines[0], "log %d", &m.logNum); err != nil {
		return manifest{}, ErrCorrupt
	}
	fields := strings.Fields(lines[1])
	if len(fields) == 0 || fields[0] != "tables" {
		return manifest{}, ErrCorrupt
	}
	for _, f := range fields[1:] {
		num, err := strconv.ParseUint(f, 10, 64)
		if err != nil {
			return manifest{}, ErrCorrupt
		}
		m.tables = append(m.tables, num)
	}
	return m, nil
}
