package serendipity

//	Allocate and zero memory. If the allocation fails, make the mallocFailed flag in the connection pointer.
//	If db != 0 and db->mallocFailed is true (indicating a prior malloc failure on the same database connection)
//	then always return 0. Hence for a particular database connection, once malloc starts failing, it fails
//	consistently until mallocFailed is reset. This is an important assumption.  There are many places in the
//	code that do things like this:
//
//			a *int = (*int)sqlite3DbMallocRaw(db, 100)
//			b *int = (*int)sqlite3DbMallocRaw(db, 200)
//			if b != nil {
//				a[10] = 9
//			}
//
//	In other words, if a subsequent malloc (ex: "b") worked, it is assumed that all prior mallocs (ex: "a") worked too.

func DbMallocRaw(sqlite3 *db, int n) ([]byte) {
	assert( db == nil || sqlite3_mutex_held(db.mutex) )
	assert( db == nil || db.pnBytesFreed == 0 )
#ifndef SQLITE_OMIT_LOOKASIDE
	if db != nil {
		if db.mallocFailed {
			return nil
		}
		var pBuf	*LookasideSlot
		if db.lookaside.bEnabled {
			if n > db.lookaside.sz {
				db.lookaside.anStat[1]++
			} else if (pBuf = db.lookaside.pFree == 0 {
				db.lookaside.anStat[2]++
			} else {
				db.lookaside.pFree = pBuf.pNext
				db.lookaside.nOut++
				db.lookaside.anStat[0]++
				if db.lookaside.nOut > db.lookaside.mxOut {
					db.lookaside.mxOut = db.lookaside.nOut
				}
				return ([]byte)(pBuf)
			}
		}
	}
#else
	if db != nil && db.mallocFailed {
		return nil
	}
#endif
	p := sqlite3Malloc(n)
	if p == nil && db {
		db.mallocFailed = 1
	}
	sqlite3MemdebugSetType(p, MEMTYPE_DB | ((db && db->lookaside.bEnabled) ? MEMTYPE_LOOKASIDE : MEMTYPE_HEAP))
	return p
}