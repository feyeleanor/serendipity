package serendipity

// Translate a single byte of Hex into an integer. This routine only works if h really is a valid hexadecimal character:  0..9a..fA..F
func HexToInt(h byte) byte {
	assert( (h >= '0' && h <= '9') ||  (h >= 'a' && h <= 'f') ||  (h >= 'A' && h <= 'F') )
	h += 9 * (1 & (h >> 6))
	return h & 0xf
}

#if !defined(SQLITE_OMIT_BLOB_LITERAL) || defined(SQLITE_HAS_CODEC)
//	Convert a BLOB literal of the form "x'hhhhhh'" into its binary value.
//	Return its binary value.
//	Space to hold the binary value has been obtained from malloc and must be freed by the calling routine.
func HexToBlob(db *sqlite3, z []byte, n int) (blob []byte) {
	blob = ([]byte)(sqlite3DbMallocRaw(db, (n / 2) + 1))
	n--
	if blob != nil {
		for i := 0; i < n; i += 2 {
			blob[i / 2] = (HexToInt(z[i]) << 4) | HexToInt(z[i + 1])
		}
		blob[i / 2] = 0
	}
	return
}
#endif /* !SQLITE_OMIT_BLOB_LITERAL || SQLITE_HAS_CODEC */
