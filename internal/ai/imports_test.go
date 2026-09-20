package ai_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestInternalAI_NoForbiddenImports verifies invariant I-015 and Rule 23:
// internal/ai must never import internal/raft, internal/storage, or internal/cluster.
// It depends strictly on internal/observability's typed surface.
func TestInternalAI_NoForbiddenImports(t *testing.T) {
	forbiddenPrefixes := []string{
		"raftkv/internal/raft",
		"raftkv/internal/storage",
		"raftkv/internal/cluster",
	}

	aiDir := "."
	entries, err := os.ReadDir(aiDir)
	if err != nil {
		t.Fatalf("failed to read internal/ai directory: %v", err)
	}

	fset := token.NewFileSet()
	goFileCount := 0

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}

		goFileCount++
		filePath := filepath.Join(aiDir, entry.Name())
		node, err := parser.ParseFile(fset, filePath, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("failed to parse %s: %v", filePath, err)
		}

		for _, imp := range node.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			for _, forbidden := range forbiddenPrefixes {
				if path == forbidden || strings.HasPrefix(path, forbidden+"/") {
					t.Errorf("I-015 / Rule 23 violation: file %s imports forbidden package %q", entry.Name(), path)
				}
			}
		}
	}

	if goFileCount == 0 {
		t.Fatalf("no .go files found in internal/ai to inspect")
	}
}
