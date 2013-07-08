package serendipity

#if defined(SQLITE_DEBUG)
	//	For debugging purposes, record when the mutex subsystem is initialized and uninitialized so that we can assert() if there is an attempt to
	//	allocate a mutex while the system is uninitialized.
	int mutexIsInit = 0;
#endif /* SQLITE_DEBUG */


//	Initialize the mutex system.
funcInitializeMutexSubsystem() (rc int) {
	if !sqlite3Config.mutex.xMutexAlloc {
		//	If the xMutexAlloc method has not been set, then the user did not install a mutex implementation via sqlite3_config() prior to 
		//	sqlite3_initialize() being called. This block copies pointers to the default implementation into the sqlite3Config structure.
		sqlite3_mutex_methods const *pFrom
		sqlite3_mutex_methods *pTo = &sqlite3Config.mutex

		pFrom = sqlite3DefaultMutex()
		memcpy(pTo, pFrom, offsetof(sqlite3_mutex_methods, xMutexAlloc))
		memcpy(&pTo.xMutexFree, &pFrom.xMutexFree, sizeof(*pTo) - offsetof(sqlite3_mutex_methods, xMutexFree))
		pTo.xMutexAlloc = pFrom.xMutexAlloc
	}
	rc = sqlite3Config.mutex.Initialise()
#ifdef SQLITE_DEBUG
	mutexIsInit = true
#endif
	return
}


//	Shutdown the mutex system. This call frees resources allocated by InitializeMutexSubsystem().
int sqlite3MutexEnd(void){
  int rc = SQLITE_OK;
  if( sqlite3Config.mutex.End ){
    rc = sqlite3Config.mutex.End();
  }

#ifdef SQLITE_DEBUG
  mutexIsInit = false
#endif

  return rc;
}

/*
** Retrieve a pointer to a mutex or allocate a new dynamic one.
*/
 sqlite3_mutex *sqlite3_mutex_alloc(int id){
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlite3_initialize() ) return 0;
#endif
  return sqlite3Config.mutex.xMutexAlloc(id);
}

func NewMutex(id int) (m *sqlite3_mutex) {
	assert( mutexIsInit )
	return sqlite3Config.mutex.xMutexAlloc(id)
}

//	Free a dynamic mutex.
void sqlite3_mutex_free(sqlite3_mutex *p){
	if p != nil {
		sqlite3Config.mutex.xMutexFree(p)
	}
}

/*
** Obtain the mutex p. If some other thread already has the mutex, block
** until it can be obtained.
*/
 void sqlite3_mutex_enter(sqlite3_mutex *p){
  if( p ){
    sqlite3Config.mutex.xMutexEnter(p);
  }
}

void (p *sqlite3_mutex) CriticalSection(f func() {
	if p != nil {
		sqlite3Config.mutex.xMutexEnter(p)
		f()
		sqlite3Config.mutex.xMutexLeave(p)
	}
}

void (p *sqlite3_mutex) CriticalSectionExemption(f func() {
	if p != nil {
		sqlite3Config.mutex.xMutexLeave(p)
		f()
		sqlite3Config.mutex.xMutexEnter(p)
	}
})

/*
** Obtain the mutex p. If successful, return SQLITE_OK. Otherwise, if another
** thread holds the mutex and it cannot be obtained, return SQLITE_BUSY.
*/
 int sqlite3_mutex_try(sqlite3_mutex *p){
  int rc = SQLITE_OK;
  if( p ){
    return sqlite3Config.mutex.xMutexTry(p);
  }
  return rc;
}

/*
** The sqlite3_mutex_leave() routine exits a mutex that was previously
** entered by the same thread.  The behavior is undefined if the mutex 
** is not currently entered. If a NULL pointer is passed as an argument
** this function is a no-op.
*/
 void sqlite3_mutex_leave(sqlite3_mutex *p){
  if( p ){
    sqlite3Config.mutex.xMutexLeave(p);
  }
}
/************** End of mutex.c ***********************************************/
/************** Begin file mutex_unix.c **************************************/
/*
** This file contains the C functions that implement mutexes for pthreads
*/

/*
** The code in this file is only used if we are compiling threadsafe
** under unix with pthreads.
**
** Note that this implementation requires a version of pthreads that
** supports recursive mutexes.
*/

/*
** Each recursive mutex is an instance of the following structure.
*/
struct sqlite3_mutex {
  pthread_mutex_t mutex;     /* Mutex controlling the lock */
  int id;                    /* Mutex type */
  volatile int nRef;         /* Number of entrances */
  volatile pthread_t owner;  /* Thread that is within this mutex */
  int trace;                 /* True to trace changes */
};
#define SQLITE3_MUTEX_INITIALIZER { PTHREAD_MUTEX_INITIALIZER, 0, 0, (pthread_t)0, 0 }

/*
** Initialize and deinitialize the mutex subsystem.
*/
int pthreadMutexInit(void){ return SQLITE_OK; }
int pthreadMutexEnd(void){ return SQLITE_OK; }

/*
** The sqlite3_mutex_alloc() routine allocates a new
** mutex and returns a pointer to it.  If it returns NULL
** that means that a mutex could not be allocated.  SQLite
** will unwind its stack and return an error.  The argument
** to sqlite3_mutex_alloc() is one of these integer constants:
**
** <ul>
** <li>  SQLITE_MUTEX_FAST
** <li>  SQLITE_MUTEX_RECURSIVE
** <li>  SQLITE_MUTEX_STATIC_MASTER
** <li>  SQLITE_MUTEX_STATIC_MEM
** <li>  SQLITE_MUTEX_STATIC_MEM2
** <li>  SQLITE_MUTEX_STATIC_PRNG
** <li>  SQLITE_MUTEX_STATIC_LRU
** <li>  SQLITE_MUTEX_STATIC_PMEM
** </ul>
**
** The first two constants cause sqlite3_mutex_alloc() to create
** a new mutex.  The new mutex is recursive when SQLITE_MUTEX_RECURSIVE
** is used but not necessarily so when SQLITE_MUTEX_FAST is used.
** The mutex implementation does not need to make a distinction
** between SQLITE_MUTEX_RECURSIVE and SQLITE_MUTEX_FAST if it does
** not want to.  But SQLite will only request a recursive mutex in
** cases where it really needs one.  If a faster non-recursive mutex
** implementation is available on the host platform, the mutex subsystem
** might return such a mutex in response to SQLITE_MUTEX_FAST.
**
** The other allowed parameters to sqlite3_mutex_alloc() each return
** a pointer to a preexisting mutex.  Six mutexes are
** used by the current version of SQLite.  Future versions of SQLite
** may add additional mutexes.  Static mutexes are for internal
** use by SQLite only.  Applications that use SQLite mutexes should
** use only the dynamic mutexes returned by SQLITE_MUTEX_FAST or
** SQLITE_MUTEX_RECURSIVE.
**
** Note that if one of the dynamic mutex parameters (SQLITE_MUTEX_FAST
** or SQLITE_MUTEX_RECURSIVE) is used then sqlite3_mutex_alloc()
** returns a different mutex on every call.  But for the 
** mutex types, the same mutex is returned on every call that has
** the same type number.
*/
sqlite3_mutex *pthreadMutexAlloc(int iType){
  sqlite3_mutex staticMutexes[] = {
    SQLITE3_MUTEX_INITIALIZER,
    SQLITE3_MUTEX_INITIALIZER,
    SQLITE3_MUTEX_INITIALIZER,
    SQLITE3_MUTEX_INITIALIZER,
    SQLITE3_MUTEX_INITIALIZER,
    SQLITE3_MUTEX_INITIALIZER
  };
  sqlite3_mutex *p;
  switch( iType ){
    case SQLITE_MUTEX_RECURSIVE: {
      p = sqlite3MallocZero( sizeof(*p) );
      if( p ){
        /* If recursive mutexes are not available, we will have to
        ** build our own.  See below. */
        pthread_mutex_init(&p->mutex, 0);
        p->id = iType;
      }
      break;
    }
    case SQLITE_MUTEX_FAST: {
      p = sqlite3MallocZero( sizeof(*p) );
      if( p ){
        p->id = iType;
        pthread_mutex_init(&p->mutex, 0);
      }
      break;
    }
    default: {
      assert( iType-2 >= 0 );
      assert( iType-2 < ArraySize(staticMutexes) );
      p = &staticMutexes[iType-2];
      p->id = iType;
      break;
    }
  }
  return p;
}


/*
** This routine deallocates a previously
** allocated mutex.  SQLite is careful to deallocate every
** mutex that it allocates.
*/
void pthreadMutexFree(sqlite3_mutex *p){
  assert( p->nRef==0 );
  assert( p->id==SQLITE_MUTEX_FAST || p->id==SQLITE_MUTEX_RECURSIVE );
  pthread_mutex_destroy(&p->mutex);
  sqlite3_free(p);
}

/*
** The sqlite3_mutex_enter() and sqlite3_mutex_try() routines attempt
** to enter a mutex.  If another thread is already within the mutex,
** sqlite3_mutex_enter() will block and sqlite3_mutex_try() will return
** SQLITE_BUSY.  The sqlite3_mutex_try() interface returns SQLITE_OK
** upon successful entry.  Mutexes created using SQLITE_MUTEX_RECURSIVE can
** be entered multiple times by the same thread.  In such cases the,
** mutex must be exited an equal number of times before another thread
** can enter.  If the same thread tries to enter any other kind of mutex
** more than once, the behavior is undefined.
*/
void pthreadMutexEnter(sqlite3_mutex *p){

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
    }else{
      pthread_mutex_lock(&p->mutex);
      assert( p->nRef==0 );
      p->owner = self;
      p->nRef = 1;
    }
  }

#ifdef SQLITE_DEBUG
  if( p->trace ){
    printf("enter mutex %p (%d) with nRef=%d\n", p, p->trace, p->nRef);
  }
#endif
}
int pthreadMutexTry(sqlite3_mutex *p){
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

/*
** The sqlite3_mutex_leave() routine exits a mutex that was
** previously entered by the same thread.  The behavior
** is undefined if the mutex is not currently entered or
** is not currently allocated.  SQLite will never do either.
*/
void pthreadMutexLeave(sqlite3_mutex *p){
  p->nRef--;
  if( p->nRef==0 ) p->owner = 0;
  assert( p->nRef==0 || p->id==SQLITE_MUTEX_RECURSIVE );

  if( p->nRef==0 ){
    pthread_mutex_unlock(&p->mutex);
  }

#ifdef SQLITE_DEBUG
  if( p->trace ){
    printf("leave mutex %p (%d) with nRef=%d\n", p, p->trace, p->nRef);
  }
#endif
}

 sqlite3_mutex_methods const *sqlite3DefaultMutex(void){
  const sqlite3_mutex_methods sMutex = {
    pthreadMutexInit,
    pthreadMutexEnd,
    pthreadMutexAlloc,
    pthreadMutexFree,
    pthreadMutexEnter,
    pthreadMutexTry,
    pthreadMutexLeave,
  };

  return &sMutex;
}