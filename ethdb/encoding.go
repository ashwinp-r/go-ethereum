package ethdb

import (
)

// Transforms b into encoding where only
// 7 bits of each byte are used to encode the bits of b
// The most significant bit is left empty, for other purposes
func Encode8to7(b []byte) []byte {
	// Calculate number of bytes in the output
	inbytes := len(b)
	outbytes := (inbytes*8 + 6)/7
	out := make([]byte, outbytes)
	outidx := 0
	for inidx := 0; inidx < inbytes; inidx++ {
		bb := b[inidx]
		switch (inidx%7) {
		case 0:
			out[outidx]   |= bb>>1
			out[outidx+1] |= (bb&0x1)<<6
		case 1:
			out[outidx+1] |= (bb>>2)&0x3f
			out[outidx+2] |= (bb&0x3)<<5
		case 2:
			out[outidx+2] |= (bb>>3)&0x1f
			out[outidx+3] |= (bb&0x7)<<4
		case 3:
			out[outidx+3] |= (bb>>4)&0xf
			out[outidx+4] |= (bb&0xf)<<3
		case 4:
			out[outidx+4] |= (bb>>5)&0x7
			out[outidx+5] |= (bb&0x1f)<<2
		case 5:
			out[outidx+5] |= (bb>>6)&0x3
			out[outidx+6] |= (bb&0x3f)<<1
		case 6:
			out[outidx+6] |= bb>>7
			out[outidx+7] |= bb&0x7f
			outidx += 8
		}
	}
	return out
}

func Decode7to8(b []byte) []byte {
	inbytes := len(b)
	outbytes := inbytes*7/8
	out := make([]byte, outbytes)
	inidx := 0
	for outidx := 0; outidx < outbytes; outidx++ {
		switch (outidx%7) {
		case 0:
			out[outidx] = ((b[inidx]&0x7f)<<1)   | ((b[inidx+1]>>6)&0x1)
		case 1:
			out[outidx] = ((b[inidx+1]&0x3f)<<2) | ((b[inidx+2]>>5)&0x3)
		case 2:
			out[outidx] = ((b[inidx+2]&0x1f)<<3) | ((b[inidx+3]>>4)&0x7)
		case 3:
			out[outidx] = ((b[inidx+3]&0xf)<<4)  | ((b[inidx+4]>>3)&0xf)
		case 4:
			out[outidx] = ((b[inidx+4]&0x7)<<5)  | ((b[inidx+5]>>2)&0x1f)
		case 5:
			out[outidx] = ((b[inidx+5]&0x3)<<6)  | ((b[inidx+6]>>1)&0x3f)
		case 6:
			out[outidx] = ((b[inidx+6]&0x1)<<7)  | (b[inidx+7]&0x7f)
			inidx += 8
		}
	}
	return out
}

func CreateBlockSuffix(blockNr uint64) []byte {
	var suffix []byte
	limit := uint64(32)
	for bytecount := 1; bytecount <=8; bytecount++ {
		if blockNr < limit {
			suffix = make([]byte, bytecount)
			b := blockNr
			for i := bytecount - 1; i > 0; i-- {
				suffix[i] = byte(b&0xff)^0xff
				b >>= 8
			}
			suffix[0] = (byte(b) | (byte(bytecount)<<5))^0xff // 3 most significant bits of the first byte are bytecount
			break
		}
		limit <<= 8
	}
	return suffix
}

func DecodeBlockSuffix(suffix []byte) uint64 {
	bytecount := int((suffix[0]^0xff)>>5)
	blockNr := uint64((suffix[0]^0xff)&0x1f)
	for i := 1; i < bytecount; i++ {
		blockNr = (blockNr<<8) | uint64(suffix[i]^0xff)
	}
	return blockNr
}