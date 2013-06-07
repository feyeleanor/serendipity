package serendipity

//	Allocate and zero memory. If the allocation fails, make the mallocFailed flag in the connection pointer.
//	If db != 0 and db->mallocFailed is true (indicating a prior malloc failure on the same database connection)
//	then always return 0. Hence for a particular database connection, once malloc starts failing, it fails
//	consistently until mallocFailed is reset. This is an important assumption.  There are many places in the
//	code that do things like this:
//
//			a *int = (*int)DbMallocRaw(db, 100)
//			b *int = (*int)DbMallocRaw(db, 200)
//			if b != nil {
//				a[10] = 9
//			}
//
//	In other words, if a subsequent malloc (ex: "b") worked, it is assumed that all prior mallocs (ex: "a") worked too.
func DbMallocRaw(sqlite3 *db, int n) ([]byte) {
	assert( db == nil || sqlite3_mutex_held(db.mutex) )
	assert( db == nil || db.pnBytesFreed == 0 )
	if db != nil && db.mallocFailed {
		return nil
	}
	p := sqlite3Malloc(n)
	if p == nil && db {
		db.mallocFailed = 1
	}
	sqlite3MemdebugSetType(p, MEMTYPE_DB | MEMTYPE_HEAP)
	return p
}


//	Resize the block of memory pointed to by p to n bytes. If the resize fails, set the mallocFailed flag in the connection object.
void *sqlite3DbRealloc(sqlite3 *db, void *p, int n) (pNew []byte){
	assert( db != nil )
	assert( sqlite3_mutex_held(db.mutex) )
	if db.mallocFailed == 0 {
		if p == nil {
			return sqlite3DbMallocRaw(db, n)
		}
		assert( sqlite3MemdebugHasType(p, MEMTYPE_DB) )
		assert( sqlite3MemdebugHasType(p, MEMTYPE_HEAP) )
		sqlite3MemdebugSetType(p, MEMTYPE_HEAP)
		pNew = sqlite3_realloc(p, n)
		if pNew == nil {
			sqlite3MemdebugSetType(p, MEMTYPE_DB | MEMTYPE_HEAP)
			db.mallocFailed = 1
		}
		sqlite3MemdebugSetType(pNew, MEMTYPE_DB | MEMTYPE_HEAP)
	}
	return
}