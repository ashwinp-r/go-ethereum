package trie

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func testValue(t *testing.T) {
	h := newHasher(false)
	hash, err := h.hash(valueNode([]byte("BLAH")), false, nil)
	if err != nil {
		t.Errorf("Could not hash %v", err)
	}
	expected := "0x0"
	got, _ := hash.(hashNode)
	if common.ToHex(got) != expected {
		t.Errorf("Expected %s, got %s", expected, common.ToHex(got))
	}
}