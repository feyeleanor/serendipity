package serendipity

import "sync"


//	Each recursive mutex is an instance of the following structure.
type RecursiveMutex struct {
	sync.Mutex
	id			int				//	mutex type
	nRef		int				//	number of entrances
	owner		int				//	Thread that is within this mutex
	trace		bool
}

struct sqlite3_mutex {
	pthread_mutex_t mutex;     /* Mutex controlling the lock */
	int id;                    /* Mutex type */
	volatile int nRef;         /* Number of entrances */
	volatile pthread_t owner;  /* Thread that is within this mutex */
	int trace;                 /* True to trace changes */
};
#define SQLITE3_MUTEX_INITIALIZER { PTHREAD_MUTEX_INITIALIZER, 0, 0, (pthread_t)0, 0 }



void (p *RecursiveMutex) CriticalSection(f func() {
	if p != nil {
		p.Enter()
	}
	f()
	if p != nil {
		p.Leave()
	}
}

void (p *RecursiveMutex) CriticalSectionExemption(f func() {
	if p != nil {
		p.Leave()
	}
	f()
	if p != nil {
		p.Enter()
	}
})

//	Obtain the mutex p. If successful, return SQLITE_OK. Otherwise, if another
// thread holds the mutex and it cannot be obtained, return SQLITE_BUSY.
func (p *RecursiveMutex) Try() (rc int) {
	if p != nil {
		return p.Try()
	}
	return
}


//	The NewMutex() routine allocates a new mutex and returns a pointer to it.  If it returns NULL that means that a mutex could not be allocated.  SQLite
//	will unwind its stack and return an error.  The argument to NewMutex() is one of these integer constants:
//
//			SQLITE_MUTEX_FAST
//			SQLITE_MUTEX_RECURSIVE
//			SQLITE_MUTEX_STATIC_MASTER
//			SQLITE_MUTEX_STATIC_MEM
//			SQLITE_MUTEX_STATIC_MEM2
//			SQLITE_MUTEX_STATIC_PRNG
//			SQLITE_MUTEX_STATIC_LRU
//			SQLITE_MUTEX_STATIC_PMEM
//
//	The first two constants cause AllocateMutex() to create a new mutex.  The new mutex is recursive when SQLITE_MUTEX_RECURSIVE
//	is used but not necessarily so when SQLITE_MUTEX_FAST is used. The mutex implementation does not need to make a distinction
//	between SQLITE_MUTEX_RECURSIVE and SQLITE_MUTEX_FAST if it does not want to.  But SQLite will only request a recursive mutex in
//	cases where it really needs one.  If a faster non-recursive mutex implementation is available on the host platform, the mutex subsystem
//	might return such a mutex in response to SQLITE_MUTEX_FAST.
//
//	The other allowed parameters to NewMutex() each return a pointer to a preexisting mutex.  Six mutexes are
//	used by the current version of SQLite.  Future versions of SQLite may add additional mutexes.  Static mutexes are for internal
//	use by SQLite only.  Applications that use SQLite mutexes should use only the dynamic mutexes returned by SQLITE_MUTEX_FAST or SQLITE_MUTEX_RECURSIVE.
//
//	Note that if one of the dynamic mutex parameters (SQLITE_MUTEX_FAST or SQLITE_MUTEX_RECURSIVE) is used then NewMutex()
//	returns a different mutex on every call.  But for the mutex types, the same mutex is returned on every call that has the same type number.
func NewMutex(iType int) (p *RecursiveMutex) {
	staticMutexes = []RecursiveMutex{
		SQLITE3_MUTEX_INITIALIZER,
		SQLITE3_MUTEX_INITIALIZER,
		SQLITE3_MUTEX_INITIALIZER,
		SQLITE3_MUTEX_INITIALIZER,
		SQLITE3_MUTEX_INITIALIZER,
		SQLITE3_MUTEX_INITIALIZER
	}

	switch iType {
	case SQLITE_MUTEX_RECURSIVE:
		if p = sqlite3MallocZero( sizeof(*p) ); p != nil {
			//	If recursive mutexes are not available, we will have to build our own.  See below.
			pthread_mutex_init(&p.mutex, 0)
			p.id = iType
		}

	case SQLITE_MUTEX_FAST:
		if p = sqlite3MallocZero( sizeof(*p) ); p != nil {
			p.id = iType
			pthread_mutex_init(&p.mutex, 0)
		}

	default:
		assert( iType - 2 >= 0 )
		assert( iType - 2 < ArraySize(staticMutexes) )
		p = &staticMutexes[iType - 2]
		p.id = iType
	}
	return
}


//	This routine deallocates a previously allocated mutex.  SQLite is careful to deallocate every mutex that it allocates.
func (p *RecursiveMutex) Free() {
	assert( p.nRef == 0 )
	assert( p.id == SQLITE_MUTEX_FAST || p.id == SQLITE_MUTEX_RECURSIVE )
	pthread_mutex_destroy(&p.mutex)
	sqlite3_free(p)
}

//	The Enter() and Try() routines attempt to enter a mutex.  If another thread is already within the mutex,
//	Enter() will block and Try() will return SQLITE_BUSY.  The Try() interface returns SQLITE_OK
//	upon successful entry.  Mutexes created using SQLITE_MUTEX_RECURSIVE can be entered multiple times by the same thread.  In such cases the,
//	mutex must be exited an equal number of times before another thread can enter.  If the same thread tries to enter any other kind of mutex
//	more than once, the behavior is undefined.
func (p *RecursiveMutex) Enter() {
	//	If recursive mutexes are not available, then we have to grow our own.  This implementation assumes that pthread_equal()
	//	is atomic - that it cannot be deceived into thinking self and p.owner are equal if p.owner changes between two values
	//	that are not equal to self while the comparison is taking place.
	//	This implementation also assumes a coherent cache - that separate processes cannot read different values from the same
	//	address at the same time.  If either of these two conditions are not met, then the mutexes will fail and problems will result.

	self := pthread_self()
	if p.nRef > 0 && pthread_equal(p.owner, self) {
		p.nRef++
	} else {
		pthread_mutex_lock(&p.mutex)
		assert( p.nRef == 0 )
		p.owner = self
		p.nRef = 1
	}

#ifdef SQLITE_DEBUG
	if p.trace != nil {
		printf("enter mutex %p (%d) with nRef=%d\n", p, p.trace, p.nRef)
	}
#endif
}

int pthreadMutexTry(RecursiveMutex *p){
  int rc;

  /* If recursive mutexes are not available, then we have to grow
  ** our own.  This implementation assumes that pthread_equal()
  ** is atomic - that it cannot be deceived into thinking self
  ** and p->owner are equal if p->owner changes between two values
  ** that are not equal to self while the comparison is taking place.
  ** This implementation also assumes a coherent cache - that 
  ** separate processes cannot read different values from the same
  ** address at the same time.  If either of these two conditions
  ** are not met, then the mutexes will fail and problems will result.
  */
  {
    pthread_t self = pthread_self();
    if( p->nRef>0 && pthread_equal(p->owner, self) ){
      p->nRef++;
      rc = SQLITE_OK;
    }else if( pthread_mutex_trylock(&p->mutex)==0 ){
      assert( p->nRef==0 );
      p->owner = self;
      p->nRef = 1;
      rc = SQLITE_OK;
    }else{
      rc = SQLITE_BUSY;
    }
  }

#ifdef SQLITE_DEBUG
  if( rc==SQLITE_OK && p->trace ){
    printf("enter mutex %p (%d) with nRef=%d\n", p, p->trace, p->nRef);
  }
#endif
  return rc;
}

//	The Leave() routine exits a mutex that was previously entered by the same thread.  The behavior
//	is undefined if the mutex is not currently entered or is not currently allocated.  SQLite will never do either.
func (p *RecursiveMutex) Leave() {
	if p != nil {
		p.nRef--
		if p.nRef == 0 {
			p.owner = 0
		}
		assert( p.nRef == 0 || p.id == SQLITE_MUTEX_RECURSIVE )

		if p.nRef == 0 {
			pthread_mutex_unlock(&p.mutex)
		}

#ifdef SQLITE_DEBUG
		if p.trace {
			printf("leave mutex %p (%d) with nRef=%d\n", p, p.trace, p.nRef)
		}
#endif
	}
}

sqlite3_mutex_methods const *sqlite3DefaultMutex(void){
  const sqlite3_mutex_methods sMutex = {
  };

  return &sMutex;
}