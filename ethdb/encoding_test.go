package ethdb_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

func TestEncodeSingleByte(t *testing.T) {
	in := []byte{79}
	out := ethdb.Encode8to7(in)
	in2 := ethdb.Decode7to8(out)
	if !bytes.Equal(in, in2) {
		t.Fatal("Decoding of encoding is not identity transformation")
	}
}

func TestEncodeRandom(t *testing.T) {
	length := int(rand.Int31n(1024))
	in := make([]byte, length)
	for i := 0; i < len(in); i++ {
		in[i] = byte(rand.Int31n(256))
	}
	out := ethdb.Encode8to7(in)
	in2 := ethdb.Decode7to8(out)
	if !bytes.Equal(in, in2) {
		t.Fatal("Decoding of encoding is not identity transformation")
	}
}
