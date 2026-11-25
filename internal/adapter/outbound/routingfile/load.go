package routingfile

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

func Load(path string) (*domain.RoutingTable, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("routing: reading %s: %w", path, err)
	}

	var file domain.RoutingFile
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&file); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("routing: %s is empty", path)
		}
		return nil, fmt.Errorf("routing: parsing %s: %w", path, err)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("routing: %s must hold one YAML document", path)
	}

	table, err := domain.NewRoutingTable(file)
	if err != nil {
		return nil, fmt.Errorf("routing: %s: %w", path, err)
	}
	return table, nil
}
