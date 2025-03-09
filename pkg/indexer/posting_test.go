package indexer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostingCompactHeader(t *testing.T) {
	pTypes := []PostingType{
		PostingTypeText,
		PostingTypeTag,
		PostingTypeEnd,
	}

	docIDLens := []uint8{0, 1, 2, 3}

	for _, pType := range pTypes {
		for _, docIDLen := range docIDLens {
			header := encodeHeader(pType, docIDLen)
			require.Equal(t, uint8(pType)*16+docIDLen, header, fmt.Sprintf("%d, %d", pType, docIDLen))

			pTypeDecoded, docIDLenDecoded := decodeHeader(header)
			require.Equal(t, pType, pTypeDecoded)
			require.Equal(t, docIDLen, docIDLenDecoded)
		}
	}
}
