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
		pNew = sqlite3_realloc(p, n)
		if pNew == nil {
			db.mallocFailed = 1
		}
	}
	return
}


//	Allocate memory.  This routine is like sqlite3_malloc() except that it assumes the memory subsystem has already been initialized.
func sqlite3Malloc(int n) (p []byte) {
	//	n <= 0	IMP: R-65312-04917
	if n <= 0 || n >= 0x7fffff00 {
		//	A memory allocation of a number of bytes which is near the maximum
		//	signed integer value might cause an integer overflow inside of the
		//	xMalloc().  Hence we limit the maximum size to 0x7fffff00, giving
		//	255 bytes of overhead.  SQLite itself will never use anything near
		//	this amount.  The only way to reach the limit is with sqlite3_malloc()
		p = nil
	} else if sqlite3Config.bMemstat {
		sqlite3_mutex_enter(mem0.mutex)
		mallocWithAlarm(n, &p)
		sqlite3_mutex_leave(mem0.mutex)
	} else {
		p = sqlite3Config.m.xMalloc(n)
	}
	assert( EIGHT_BYTE_ALIGNMENT(p) )		//	IMP: R-04675-44850
	return
}


//	This version of the memory allocation is for use by the application.
//	First make sure the memory subsystem is initialized, then do the allocation.
func sqlite3_malloc(int n) []byte {
#ifndef SQLITE_OMIT_AUTOINIT
	if sqlite3_initialize() {
		return nil
	}
#endif
	return sqlite3Malloc(n)
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
	sqlite3_mutex_leave(mem0.mutex);
	xCallback(pArg, nowUsed, nByte);
	sqlite3_mutex_enter(mem0.mutex);
	mem0.alarmCallback = xCallback;
	mem0.alarmArg = pArg;
}


//	Do a memory allocation with statistics and alarms.  Assume the lock is already held.
func mallocWithAlarm(int n, void **pp) (nFull int) {
	void *p;
	assert( sqlite3_mutex_held(mem0.mutex) );
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
#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
	if( p==0 && mem0.alarmCallback ){
		sqlite3MallocAlarm(nFull);
		p = sqlite3Config.m.xMalloc(nFull);
	}
#endif
	if p != nil {
		nFull = sqlite3MallocSize(p);
		sqlite3StatusAdd(SQLITE_STATUS_MEMORY_USED, nFull);
		sqlite3StatusAdd(SQLITE_STATUS_MALLOC_COUNT, 1);
	}
	*pp = p;
	return
}


//	Each thread may only have a single outstanding allocation from xScratchMalloc().  We verify this constraint in the single-threaded
//	case by setting scratchAllocOut to 1 when an allocation is outstanding clearing it when the allocation is freed.
#if SQLITE_THREADSAFE==0 && !defined(NDEBUG)
int scratchAllocOut = 0;
#endif


//	Allocate memory that is to be used and released right away.
//	This routine is similar to alloca() in that it is not intended for situations where the memory might be held long-term.  This
//	routine is intended to get memory to old large transient data structures that would not normally fit on the stack of an
//	embedded processor.
func sqlite3ScratchMalloc(int n) (p []byte) {
	assert( n>0 );

	sqlite3_mutex_enter(mem0.mutex);
	if mem0.nScratchFree && sqlite3Config.szScratch>=n {
		p = mem0.pScratchFree;
		mem0.pScratchFree = mem0.pScratchFree->pNext;
		mem0.nScratchFree--;
		sqlite3StatusAdd(SQLITE_STATUS_SCRATCH_USED, 1);
		sqlite3StatusSet(SQLITE_STATUS_SCRATCH_SIZE, n);
		sqlite3_mutex_leave(mem0.mutex);
	} else {
		if( sqlite3Config.bMemstat ){
			sqlite3StatusSet(SQLITE_STATUS_SCRATCH_SIZE, n);
			n = mallocWithAlarm(n, &p);
			if( p ) sqlite3StatusAdd(SQLITE_STATUS_SCRATCH_OVERFLOW, n);
			sqlite3_mutex_leave(mem0.mutex);
		} else {
			sqlite3_mutex_leave(mem0.mutex);
			p = sqlite3Config.m.xMalloc(n);
		}
	}
	assert( sqlite3_mutex_notheld(mem0.mutex) );

#if SQLITE_THREADSAFE==0 && !defined(NDEBUG)
	//	Verify that no more than two scratch allocations per thread are outstanding at one time.  (This is only checked in the
	//	single-threaded case since checking in the multi-threaded case would be much more complicated.)
	assert( scratchAllocOut<=1 );
	if( p ) scratchAllocOut++;
#endif

	return p;
}

func sqlite3ScratchFree(void *p) {
	if p {
#if SQLITE_THREADSAFE==0 && !defined(NDEBUG)
		//	Verify that no more than two scratch allocation per thread is outstanding at one time.  (This is only checked in the
		//	single-threaded case since checking in the multi-threaded case would be much more complicated.)
		assert( scratchAllocOut>=1 && scratchAllocOut<=2 );
		scratchAllocOut--;
#endif

		if p >= sqlite3Config.pScratch && p < mem0.pScratchEnd {
			//	Release memory from the SQLITE_CONFIG_SCRATCH allocation
			ScratchFreeslot *pSlot;
			pSlot = (ScratchFreeslot*)p;
			sqlite3_mutex_enter(mem0.mutex);
			pSlot->pNext = mem0.pScratchFree;
			mem0.pScratchFree = pSlot;
			mem0.nScratchFree++;
			assert( mem0.nScratchFree <= (u32)sqlite3Config.nScratch );
			sqlite3StatusAdd(SQLITE_STATUS_SCRATCH_USED, -1);
			sqlite3_mutex_leave(mem0.mutex);
		} else {
			//	Release memory back to the heap
			if sqlite3Config.bMemstat {
				int iSize = sqlite3MallocSize(p);
				sqlite3_mutex_enter(mem0.mutex);
				sqlite3StatusAdd(SQLITE_STATUS_SCRATCH_OVERFLOW, -iSize);
				sqlite3StatusAdd(SQLITE_STATUS_MEMORY_USED, -iSize);
				sqlite3StatusAdd(SQLITE_STATUS_MALLOC_COUNT, -1);
				sqlite3Config.m.xFree(p);
				sqlite3_mutex_leave(mem0.mutex);
			} else {
				sqlite3Config.m.xFree(p);
			}
		}
	}
}