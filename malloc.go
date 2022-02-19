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
	assert( db == nil || db.pnBytesFreed == 0 )
	if db != nil && db.mallocFailed {
		return nil
	}
	p := sqlite3Malloc(n)
	if p == nil && db {
		db.mallocFailed = 1
	}
	return p
}


//	Resize the block of memory pointed to by p to n bytes. If the resize fails, set the mallocFailed flag in the connection object.
void *sqlite3DbRealloc(sqlite3 *db, void *p, int n) (pNew []byte){
	assert( db != nil )
	if db.mallocFailed == 0 {
		if p == nil {
			return sqlite3DbMallocRaw(db, n)
		}
		pNew = sqlite3_realloc(p, n)
		if pNew == nil {
			db.mallocFailed = 1
		}
	}
	return
}


//	Allocate memory.  This routine is like sqlite3Malloc() except that it assumes the memory subsystem has already been initialized.
func sqlite3Malloc(int n) (p []byte) {
	//	n <= 0	IMP: R-65312-04917
	if n <= 0 || n >= 0x7fffff00 {
		//	A memory allocation of a number of bytes which is near the maximum
		//	signed integer value might cause an integer overflow inside of the
		//	xMalloc().  Hence we limit the maximum size to 0x7fffff00, giving
		//	255 bytes of overhead.  SQLite itself will never use anything near
		//	this amount.  The only way to reach the limit is with sqlite3Malloc()
		p = nil
	} else {
		p = sqlite3Config.m.xMalloc(n)
	}
	assert( EIGHT_BYTE_ALIGNMENT(p) )		//	IMP: R-04675-44850
	return
}


//	Trigger the alarm 
func sqlite3MallocAlarm(int nByte) {
	void (*xCallback)(void*,sqlite3_int64,int);
	sqlite3_int64 nowUsed;
	void *pArg;
	if mem0.alarmCallback == 0 {
		return
	}
	xCallback = mem0.alarmCallback;
	nowUsed = sqlite3StatusValue(SQLITE_STATUS_MEMORY_USED);
	pArg = mem0.alarmArg;
	mem0.alarmCallback = 0;
	mem0.mutex.Leave()
	xCallback(pArg, nowUsed, nByte);
	mem0.mutex.Enter()
	mem0.alarmCallback = xCallback;
	mem0.alarmArg = pArg;
}


//	Do a memory allocation with statistics and alarms.  Assume the lock is already held.
func mallocWithAlarm(int n, void **pp) (nFull int) {
	void *p;
	nFull = sqlite3Config.m.xRoundup(n);
	sqlite3StatusSet(SQLITE_STATUS_MALLOC_SIZE, n);
	if mem0.alarmCallback != 0 {
		int nUsed = sqlite3StatusValue(SQLITE_STATUS_MEMORY_USED);
		if nUsed >= mem0.alarmThreshold - nFull {
			mem0.nearlyFull = 1;
			sqlite3MallocAlarm(nFull);
		} else {
			mem0.nearlyFull = 0;
		}
	}
	p = sqlite3Config.m.xMalloc(nFull);
	if p != nil {
		nFull = sqlite3MallocSize(p);
		sqlite3StatusAdd(SQLITE_STATUS_MEMORY_USED, nFull);
		sqlite3StatusAdd(SQLITE_STATUS_MALLOC_COUNT, 1);
	}
	*pp = p;
	return
}


//	Allocate memory that is to be used and released right away.
//	This routine is similar to alloca() in that it is not intended for situations where the memory might be held long-term.  This
//	routine is intended to get memory to old large transient data structures that would not normally fit on the stack of an
//	embedded processor.
func sqlite3ScratchMalloc(int n) (p []byte) {
	mem0.mutex.Enter()
	if mem0.nScratchFree != 0 && sqlite3Config.szScratch >= n {
		p = mem0.pScratchFree
		mem0.pScratchFree = mem0.pScratchFree.pNext
		mem0.nScratchFree--
		sqlite3StatusAdd(SQLITE_STATUS_SCRATCH_USED, 1)
		sqlite3StatusSet(SQLITE_STATUS_SCRATCH_SIZE, n)
		mem0.mutex.Leave()
	} else {
		mem0.mutex.Leave()
		p = sqlite3Config.m.xMalloc(n)
	}
	return p
}

func sqlite3ScratchFree(void *p) {
	if p != nil {
		if p >= sqlite3Config.pScratch && p < mem0.pScratchEnd {
			//	Release memory from the SQLITE_CONFIG_SCRATCH allocation
			ScratchFreeslot *pSlot;
			pSlot = (ScratchFreeslot*)p;
			mem0.mutex.CriticalSection(func() {
				pSlot->pNext = mem0.pScratchFree;
				mem0.pScratchFree = pSlot;
				mem0.nScratchFree++;
				assert( mem0.nScratchFree <= (u32)sqlite3Config.nScratch );
				sqlite3StatusAdd(SQLITE_STATUS_SCRATCH_USED, -1);
			})
		} else {
			//	Release memory back to the heap
			sqlite3Config.m.xFree(p);
		}
	}
}