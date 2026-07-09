package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestTokenGenerate(t *testing.T) {
	root := Build("test")
	buf := &bytes.Buffer{}
	root.SetOut(buf)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"token", "--generate"})

	if err := root.Execute(); err != nil {
		t.Fatalf("token --generate: %v", err)
	}
	tok := strings.TrimSpace(buf.String())
	if len(tok) != 64 {
		t.Errorf("generated token length = %d, want 64 hex chars", len(tok))
	}
}

func TestTokenGenerateUnique(t *testing.T) {
	tokens := make(map[string]bool, 5)
	for range 5 {
		tok, err := randomToken()
		if err != nil {
			t.Fatal(err)
		}
		if tokens[tok] {
			t.Fatal("duplicate token generated")
		}
		tokens[tok] = true
	}
}

func TestTokenShowNoConfig(t *testing.T) {
	root := Build("test")
	root.SetOut(&bytes.Buffer{})
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"token"}) // no --config, no --generate

	err := root.Execute()
	if err == nil {
		t.Fatal("expected error when auth_token not set, got nil")
	}
}
