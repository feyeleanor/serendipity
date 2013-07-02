package serendipity

/*
** This structure encapsulates a user-function destructor callback (as
** configured using create_function_v2()) and a reference counter. When
** create_function_v2() is called to create a function with a destructor,
** a single object of this type is allocated. The Function.pDestructor
** member of each of the new FuncDef objects is set to point to the allocated
** FunctionDestructor.
**
** Thereafter, when one of the FuncDef objects is deleted, the reference
** count on this object is decremented. When it reaches 0, the destructor
** is invoked and the FuncDestructor structure freed.
*/
struct FunctionDestructor {
  void (*xDestroy)(void *)
  void *UserData
};


/*
** Each SQL function is defined by an instance of the following
** structure.  A pointer to this structure is stored in the sqlite.aFunc
** hash table.  When multiple functions have the same name, the hash table
** points to a linked list of these structures.
*/
struct FuncDef {
  i16 nArg;            /* Number of arguments.  -1 means unlimited */
  u8 flags;            /* Some combination of SQLITE_FUNC_* */
  void *UserData;     /* User data parameter */
  FuncDef *pNext;      /* Next function with same name */
  void (*xFunc)(Context*,int,sqlite3_value**); /* Regular function */
  void (*xStep)(Context*,int,sqlite3_value**); /* Aggregate step */
  void (*xFinalize)(Context*);                /* Aggregate finalizer */
  char *zName;         /* SQL name of the function. */
  FuncDef *pHash;      /* Next with a different name but the same hash */
  FuncDestructor *pDestructor;   /* Reference counted destructor function */
};

type Function struct {
	Name		string			//	SQL name of the function
	Arguments	int				//	-1 means unlimited
	flags		byte			//	Some combination of SQLITE_FUNC_* flags
	UserData	interface{}
	Func		func(*Context, []*sqlite_value)		//	Regular function
	Step		func(*Context, []*sqlite_value)		//	Aggregate step
	Finalize	func(*Context)						//	Aggregate finalizer
	*FunctionDestructor
}


/*
** Possible values for FuncDef.flags.  Note that the _LENGTH and _TYPEOF
** values must correspond to OPFLAG_LENGTHARG and OPFLAG_TYPEOFARG.  There
** are assert() statements in the code to verify this.
*/
#define SQLITE_FUNC_LIKE     0x01 /* Candidate for the LIKE optimization */
#define SQLITE_FUNC_CASE     0x02 /* Case-sensitive LIKE-type function */
#define SQLITE_FUNC_EPHEM    0x04 /* Ephemeral.  Delete with VDBE */
#define SQLITE_FUNC_NEEDCOLL 0x08 /* sqlite3GetFuncCollSeq() might be called */
#define SQLITE_FUNC_COUNT    0x10 /* Built-in count(*) aggregate */
#define SQLITE_FUNC_COALESCE 0x20 /* Built-in coalesce() or ifnull() function */
#define SQLITE_FUNC_LENGTH   0x40 /* Built-in length() function */
#define SQLITE_FUNC_TYPEOF   0x80 /* Built-in typeof() function */

/*
** The following three macros, FUNCTION(), LIKEFUNC() and AGGREGATE() are
** used to create the initializers for the FuncDef structures.
**
**   FUNCTION(zName, nArg, iArg, bNC, xFunc)
**     Used to create a scalar function definition of a function zName 
**     implemented by C function xFunc that accepts nArg arguments. The
**     value passed as iArg is cast to a (void*) and made available
**     as the user-data (sqlite3_user_data()) for the function. If 
**     argument bNC is true, then the SQLITE_FUNC_NEEDCOLL flag is set.
**
**   AGGREGATE(zName, nArg, iArg, bNC, xStep, xFinal)
**     Used to create an aggregate function definition implemented by
**     the C functions xStep and xFinal. The first four parameters
**     are interpreted in the same way as the first 4 parameters to
**     FUNCTION().
**
**   LIKEFUNC(zName, nArg, pArg, flags)
**     Used to create a scalar function definition of a function zName 
**     that accepts nArg arguments and is implemented by a call to C 
**     function sql_like. Argument pArg is cast to a (void *) and made
**     available as the function user-data (sqlite3_user_data()). The
**     FuncDef.flags variable is set to the value passed as the flags
**     parameter.
*/
#define FUNCTION(zName, nArg, iArg, bNC, xFunc) {nArg, bNC * SQLITE_FUNC_NEEDCOLL, SQLITE_INT_TO_PTR(iArg), nil, xFunc, nil, nil, zName, nil, nil}
#define FUNCTION2(zName, nArg, iArg, bNC, xFunc, extraFlags) {nArg, bNC * SQLITE_FUNC_NEEDCOLL | extraFlags, SQLITE_INT_TO_PTR(iArg), nil, xFunc, nil, nil, zName, nil, nil}
#define STR_FUNCTION(zName, nArg, pArg, bNC, xFunc) {nArg, bNC * SQLITE_FUNC_NEEDCOLL, pArg, nil, xFunc, nil, nil, zName, nil, nil}
#define LIKEFUNC(zName, nArg, arg, flags) {nArg, flags, (void *)arg, nil, sql_like, nil, nil, zName, nil, nil}
#define AGGREGATE(zName, nArg, arg, nc, xStep, xFinal) {nArg, nc * SQLITE_FUNC_NEEDCOLL, SQLITE_INT_TO_PTR(arg), nil, nil, xStep,xFinal , zName , nil, nil}

/* During the search for the best function definition, this procedure
** is called to test how well the function passed as the first argument
** matches the request for a function with nArg arguments. The value returned indicates how well the
** request is matched. A higher value indicates a better match.
**
** If nArg is -1 that means to only return a match (non-zero) if p->nArg
** is also -1.  In other words, we are searching for a function that
** takes a variable number of arguments.
**
** If nArg is -2 that means that we are searching for any function 
** regardless of the number of arguments it uses, so return a positive
** match score for any
**
** The returned value is always between 0 and 6, as follows:
**
** 0: Not a match.
** 1: function takes any number of arguments
** 2: argument count match exactly.
**
** If nArg==(-2) then any function with a non-null xStep or xFunc is
** a perfect match and any function with both xStep and xFunc NULL is
** a non-match.
*/
const (
	FUNC_NO_MATCH		= 0
	FUNC_PERFECT_MATCH  = 2
)

func (f *Function) MatchQuality(args int) (r int) {		/* nArg		Desired number of arguments.  -1 == any */
	if args == -2 {			//	-2 is a special case
		if f.Func == nil && f.Step == nil {
			return FUNC_NO_MATCH
		} else {
			return FUNC_PERFECT_MATCH
		}
	}

	if f.Arguments != args && f.Arguments >= 0 {
		return FUNC_NO_MATCH
	}

	//	Give a better score to a function with a specific number of arguments than to function that accepts any number of arguments.
	if f.Arguments == args {
		r = 2
	} else {
		r = 1
	}
	return
}

/*
** Invoke the destructor function associated with FuncDef p, if any. Except,
** if this is not the last copy of the function, do not invoke it.
*/
func (f *Function) Destroy() {
	if destructor := f.pDestructor; destructor != nil {
		if destructor.nRef--; destructor.nRef == 0 {
			destructor.xDestroy(destructor.UserData)
		}
	}
}

/*
** Return the collating function associated with a function.
*/
CollationSequence *sqlite3GetFuncCollSeq(Context *context){
	return context.pColl
}

/*
** Indicate that the accumulator load should be skipped on this
** iteration of the aggregate loop.
*/
void sqlite3SkipAccumulatorLoad(Context *context){
	context.skipFlag = true
}

/*
** Implementation of the non-aggregate min() and max() functions
*/
void minmaxFunc(
	Context *context,
	int argc,
	sqlite3_value **argv
){
	int i;
	int mask;    /* 0 for min() or 0xffffffff for max() */
	int iBest;
	CollationSequence *pColl;

  assert( argc>1 );
  mask = sqlite3_user_data(context)==0 ? 0 : -1;
  pColl = sqlite3GetFuncCollSeq(context);
  assert( pColl );
  assert( mask==-1 || mask==0 );
  iBest = 0;
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ) return;
  for(i=1; i<argc; i++){
    if( sqlite3_value_type(argv[i])==SQLITE_NULL ) return;
    if( (sqlite3MemCompare(argv[iBest], argv[i], pColl)^mask)>=0 ){
      iBest = i;
    }
  }
  sqlite3_result_value(context, argv[iBest]);
}

/*
** Return the type of the argument.
*/
void typeofFunc(
  Context *context,
  int NotUsed,
  sqlite3_value **argv
){
  const char *z = 0;
  UNUSED_PARAMETER(NotUsed);
  switch( sqlite3_value_type(argv[0]) ){
    case SQLITE_INTEGER: z = "integer"; break;
    case SQLITE_TEXT:    z = "text";    break;
    case SQLITE_FLOAT:   z = "real";    break;
    case SQLITE_BLOB:    z = "blob";    break;
    default:             z = "null";    break;
  }
  sqlite3_result_text(context, z, -1, SQLITE_STATIC);
}


/*
** Implementation of the length() function
*/
void lengthFunc(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  int len;

  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  switch( sqlite3_value_type(argv[0]) ){
    case SQLITE_BLOB:
    case SQLITE_INTEGER:
    case SQLITE_FLOAT: {
      sqlite3_result_int(context, sqlite3_value_bytes(argv[0]));
      break;
    }
    case SQLITE_TEXT: {
      z := argv[0].Text()
      if( z==0 ) return;
      len = 0;
      while( *z ){
        len++;
        SQLITE_SKIP_UTF8(z);
      }
      sqlite3_result_int(context, len);
      break;
    }
    default: {
      sqlite3_result_null(context);
      break;
    }
  }
}

/*
** Implementation of the abs() function.
**
** IMP: R-23979-26855 The abs(X) function returns the absolute value of
** the numeric argument X. 
*/
void absFunc(Context *context, int argc, sqlite3_value **argv){
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  switch( sqlite3_value_type(argv[0]) ){
    case SQLITE_INTEGER: {
      i64 iVal = sqlite3_value_int64(argv[0]);
      if( iVal<0 ){
        if( (iVal<<1)==0 ){
          /* IMP: R-35460-15084 If X is the integer -9223372036854775807 then
          ** abs(X) throws an integer overflow error since there is no
          ** equivalent positive 64-bit two complement value. */
          sqlite3_result_error(context, "integer overflow", -1);
          return;
        }
        iVal = -iVal;
      } 
      sqlite3_result_int64(context, iVal);
      break;
    }
    case SQLITE_NULL: {
      /* IMP: R-37434-19929 Abs(X) returns NULL if X is NULL. */
      sqlite3_result_null(context);
      break;
    }
    default: {
      /* Because sqlite3_value_float64() returns 0.0 if the argument is not
      ** something that can be converted into a number, we have:
      ** IMP: R-57326-31541 Abs(X) return 0.0 if X is a string or blob that
      ** cannot be converted to a numeric value. 
      */
      float64 rVal = sqlite3_value_float64(argv[0]);
      if( rVal<0 ) rVal = -rVal;
      sqlite3_result_float64(context, rVal);
      break;
    }
  }
}

/*
** Implementation of the instr() function.
**
** instr(haystack,needle) finds the first occurrence of needle
** in haystack and returns the number of previous characters plus 1,
** or 0 if needle does not occur within haystack.
**
** If both haystack and needle are BLOBs, then the result is one more than
** the number of bytes in haystack prior to the first occurrence of needle,
** or 0 if needle never occurs in haystack.
*/
void instrFunc(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zHaystack;
  const unsigned char *zNeedle;
  int nHaystack;
  int nNeedle;
  int typeHaystack, typeNeedle;
  int N = 1;
  int isText;

  UNUSED_PARAMETER(argc);
  typeHaystack = sqlite3_value_type(argv[0]);
  typeNeedle = sqlite3_value_type(argv[1]);
  if( typeHaystack==SQLITE_NULL || typeNeedle==SQLITE_NULL ) return;
  nHaystack = sqlite3_value_bytes(argv[0]);
  nNeedle = sqlite3_value_bytes(argv[1]);
  if( typeHaystack==SQLITE_BLOB && typeNeedle==SQLITE_BLOB ){
    zHaystack = sqlite3_value_blob(argv[0]);
    zNeedle = sqlite3_value_blob(argv[1]);
    isText = 0;
  }else{
    zHaystack = argv[0].Text()
    zNeedle = argv[1].Text()
    isText = 1;
  }
  while( nNeedle<=nHaystack && memcmp(zHaystack, zNeedle, nNeedle)!=0 ){
    N++;
    do{
      nHaystack--;
      zHaystack++;
    }while( isText && (zHaystack[0]&0xc0)==0x80 );
  }
  if( nNeedle>nHaystack ) N = 0;
  sqlite3_result_int(context, N);
}

/*
** Implementation of the substr() function.
**
** substr(x,p1,p2)  returns p2 characters of x[] beginning with p1.
** p1 is 1-indexed.  So substr(x,1,1) returns the first character
** of x.  If x is text, then we actually count UTF-8 characters.
** If x is a blob, then we count bytes.
**
** If p1 is negative, then we begin abs(p1) from the end of x[].
**
** If p2 is negative, return the p2 characters preceeding p1.
*/
void substrFunc(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *z;
  const unsigned char *z2;
  int len;
  int p0type;
  i64 p1, p2;
  int negP2 = 0;

  assert( argc==3 || argc==2 );
  if( sqlite3_value_type(argv[1])==SQLITE_NULL
   || (argc==3 && sqlite3_value_type(argv[2])==SQLITE_NULL)
  ){
    return;
  }
  p0type = sqlite3_value_type(argv[0]);
  p1 = sqlite3_value_int(argv[1]);
  if( p0type==SQLITE_BLOB ){
    len = sqlite3_value_bytes(argv[0]);
    z = sqlite3_value_blob(argv[0]);
    if( z==0 ) return;
    assert( len==sqlite3_value_bytes(argv[0]) );
  }else{
    z = argv[0].Text()
    if( z==0 ) return;
    len = 0;
    if( p1<0 ){
      for(z2=z; *z2; len++){
        SQLITE_SKIP_UTF8(z2);
      }
    }
  }
  if( argc==3 ){
    p2 = sqlite3_value_int(argv[2]);
    if( p2<0 ){
      p2 = -p2;
      negP2 = 1;
    }
  }else{
    p2 = Context_db_handle(context)->aLimit[SQLITE_LIMIT_LENGTH];
  }
  if( p1<0 ){
    p1 += len;
    if( p1<0 ){
      p2 += p1;
      if( p2<0 ) p2 = 0;
      p1 = 0;
    }
  }else if( p1>0 ){
    p1--;
  }else if( p2>0 ){
    p2--;
  }
  if( negP2 ){
    p1 -= p2;
    if( p1<0 ){
      p2 += p1;
      p1 = 0;
    }
  }
  assert( p1>=0 && p2>=0 );
  if( p0type!=SQLITE_BLOB ){
    while( *z && p1 ){
      SQLITE_SKIP_UTF8(z);
      p1--;
    }
    for(z2=z; *z2 && p2; p2--){
      SQLITE_SKIP_UTF8(z2);
    }
    sqlite3_result_text(context, (char*)z, (int)(z2-z), SQLITE_TRANSIENT);
  }else{
    if( p1+p2>len ){
      p2 = len-p1;
      if( p2<0 ) p2 = 0;
    }
    context.sqlite3_result_blob((char*)&z[p1:p2], SQLITE_TRANSIENT)
  }
}

/*
** Implementation of the round() function
*/
void roundFunc(Context *context, int argc, sqlite3_value **argv){
  int n = 0;
  float64 r;
  char *zBuf;
  assert( argc==1 || argc==2 );
  if( argc==2 ){
    if( SQLITE_NULL==sqlite3_value_type(argv[1]) ) return;
    n = sqlite3_value_int(argv[1]);
    if( n>30 ) n = 30;
    if( n<0 ) n = 0;
  }
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ) return;
  r = sqlite3_value_float64(argv[0]);
  /* If Y==0 and X will fit in a 64-bit int,
  ** handle the rounding directly,
  ** otherwise use printf.
  */
  if( n==0 && r>=0 && r<LARGEST_INT64-1 ){
    r = (float64)((sqlite_int64)(r+0.5));
  }else if( n==0 && r<0 && (-r)<LARGEST_INT64-1 ){
    r = -(float64)((sqlite_int64)((-r)+0.5));
  }else{
    zBuf = sqlite3_mprintf("%.*f",n,r);
    if( zBuf==0 ){
      sqlite3_result_error_nomem(context);
      return;
    }
    sqlite3AtoF(zBuf, &r, len(zBuf))
    sqlite3_free(zBuf);
  }
  sqlite3_result_float64(context, r);
}

/*
** Allocate nByte bytes of space using sqlite3_malloc(). If the
** allocation fails, call sqlite3_result_error_nomem() to notify
** the database handle that malloc() has failed and return NULL.
** If nByte is larger than the maximum string or blob length, then
** raise an SQLITE_TOOBIG exception and return NULL.
*/
void *contextMalloc(Context *context, i64 nByte){
  char *z;
  sqlite3 *db = Context_db_handle(context);
  assert( nByte>0 );
  if( nByte>db->aLimit[SQLITE_LIMIT_LENGTH] ){
    sqlite3_result_error_toobig(context);
    z = 0;
  }else{
    z = sqlite3Malloc((int)nByte);
    if( !z ){
      sqlite3_result_error_nomem(context);
    }
  }
  return z;
}

/*
** Implementation of the upper() and lower() SQL functions.
*/
void upperFunc(Context *context, int argc, sqlite3_value **argv){
  char *z1;
  const char *z2;
  int i, n;
  UNUSED_PARAMETER(argc);
  z2 = argv[0].Text()
  n = sqlite3_value_bytes(argv[0]);
  /* Verify that the call to _bytes() does not invalidate the _text() pointer */
  assert( z2 == argv[0].Text() );
  if( z2 ){
    z1 = contextMalloc(context, ((i64)n)+1);
    if( z1 ){
      for(i=0; i<n; i++){
        z1[i] = strings.ToUpper(z2[i:])[0]
      }
      sqlite3_result_text(context, z1, n, sqlite3_free);
    }
  }
}
void lowerFunc(Context *context, int argc, sqlite3_value **argv){
  char *z1;
  const char *z2;
  int i, n;
  UNUSED_PARAMETER(argc);
  z2 = argv[0].Text()
  n = sqlite3_value_bytes(argv[0]);
  /* Verify that the call to _bytes() does not invalidate the _text() pointer */
  assert( z2 == argv[0].Text() );
  if( z2 ){
    z1 = contextMalloc(context, ((i64)n)+1);
    if( z1 ){
      for(i=0; i<n; i++){
        z1[i] = strings.ToLower(z2[i])
      }
      sqlite3_result_text(context, z1, n, sqlite3_free);
    }
  }
}

/*
** The COALESCE() and IFNULL() functions are implemented as VDBE code so
** that unused argument values do not have to be computed.  However, we
** still need some kind of function implementation for this routines in
** the function table.  That function implementation will never be called
** so it doesn't matter what the implementation is.  We might as well use
** the "version()" function as a substitute.
*/
#define ifnullFunc versionFunc   /* Substitute function - never called */

/*
** Implementation of random().  Return a random integer.  
*/
void randomFunc(
  Context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  sqlite_int64 r;
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  sqlite3_randomness(sizeof(r), &r);
  if( r<0 ){
    /* We need to prevent a random number of 0x8000000000000000 
    ** (or -9223372036854775808) since when you do abs() of that
    ** number of you get the same value back again.  To do this
    ** in a way that is testable, mask the sign bit off of negative
    ** values, resulting in a positive value.  Then take the 
    ** 2s complement of that positive value.  The end result can
    ** therefore be no less than -9223372036854775807.
    */
    r = -(r & LARGEST_INT64);
  }
  sqlite3_result_int64(context, r);
}

/*
** Implementation of randomblob(N).  Return a random blob
** that is N bytes long.
*/
void randomBlob(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  int n;
  unsigned char *p;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  n = sqlite3_value_int(argv[0]);
  if( n<1 ){
    n = 1;
  }
  p = contextMalloc(context, n);
  if( p ){
    sqlite3_randomness(n, p);
    context.sqlite3_result_blob((char*)p, sqlite3_free)
  }
}

/*
** Implementation of the last_insert_rowid() SQL function.  The return
** value is the same as the sqlite3_last_insert_rowid() API function.
*/
void last_insert_rowid(
  Context *context, 
  int NotUsed, 
  sqlite3_value **NotUsed2
){
  sqlite3 *db = Context_db_handle(context);
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  /* IMP: R-51513-12026 The last_insert_rowid() SQL function is a
  ** wrapper around the sqlite3_last_insert_rowid() C/C++ interface
  ** function. */
  sqlite3_result_int64(context, sqlite3_last_insert_rowid(db));
}

/*
** Implementation of the changes() SQL function.
**
** IMP: R-62073-11209 The changes() SQL function is a wrapper
** around the sqlite3_changes() C/C++ function and hence follows the same
** rules for counting changes.
*/
void changes(
  Context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  sqlite3 *db = Context_db_handle(context);
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  sqlite3_result_int(context, sqlite3_changes(db));
}

/*
** Implementation of the total_changes() SQL function.  The return value is
** the same as the sqlite3_total_changes() API function.
*/
void total_changes(
  Context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  sqlite3 *db = Context_db_handle(context);
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  /* IMP: R-52756-41993 This function is a wrapper around the
  ** sqlite3_total_changes() C/C++ interface. */
  sqlite3_result_int(context, sqlite3_total_changes(db));
}

/*
** A structure defining how to do GLOB-style comparisons.
*/
struct compareInfo {
  u8 matchAll;
  u8 matchOne;
  u8 matchSet;
  u8 noCase;
};

# define GlogUpperToLower(A)   if !((A) & ~0x7f) { A = strings.ToLower(A) }

const struct compareInfo globInfo = { '*', '?', '[', 0 };
/* The correct SQL-92 behavior is for the LIKE operator to ignore
** case.  Thus  'a' LIKE 'A' would be true. */
const struct compareInfo likeInfoNorm = { '%', '_',   0, 1 };
/* If SQLITE_CASE_SENSITIVE_LIKE is defined, then the LIKE operator
** is case sensitive causing 'a' LIKE 'A' to be false */
const struct compareInfo likeInfoAlt = { '%', '_',   0, 0 };

/*
** Compare two UTF-8 strings for equality where the first string can
** potentially be a "glob" expression.  Return true (1) if they
** are the same and false (0) if they are different.
**
** Globbing rules:
**
**      '*'       Matches any sequence of zero or more characters.
**
**      '?'       Matches exactly one character.
**
**     [...]      Matches one character from the enclosed list of
**                characters.
**
**     [^...]     Matches one character not in the enclosed list.
**
** With the [...] and [^...] matching, a ']' character can be included
** in the list by making it the first character after '[' or '^'.  A
** range of characters can be specified using '-'.  Example:
** "[a-z]" matches any single lower-case letter.  To match a '-', make
** it the last character in the list.
**
** This routine is usually quick, but can be N**2 in the worst case.
**
** Hints: to match '*' or '?', put them in "[]".  Like this:
**
**         abc[*]xyz        Matches "abc*xyz" only
*/
int patternCompare(
  const u8 *zPattern,              /* The glob pattern */
  const u8 *zString,               /* The string to compare against the glob */
  const struct compareInfo *pInfo, /* Information about how to do the compare */
  u32 esc                          /* The escape character */
){
  u32 c, c2;
  int invert;
  int seen;
  u8 matchOne = pInfo->matchOne;
  u8 matchAll = pInfo->matchAll;
  u8 matchSet = pInfo->matchSet;
  u8 noCase = pInfo->noCase; 
  int prevEscape = 0;     /* True if the previous character was 'escape' */

  while( (c = sqlite3Utf8Read(&zPattern))!=0 ){
    if( c==matchAll && !prevEscape ){
      while( (c=sqlite3Utf8Read(&zPattern)) == matchAll
               || c == matchOne ){
        if( c==matchOne && sqlite3Utf8Read(&zString)==0 ){
          return 0;
        }
      }
      if( c==0 ){
        return 1;
      }else if( c==esc ){
        c = sqlite3Utf8Read(&zPattern);
        if( c==0 ){
          return 0;
        }
      }else if( c==matchSet ){
        assert( esc==0 );         /* This is GLOB, not LIKE */
        assert( matchSet<0x80 );  /* '[' is a single-byte character */
        while( *zString && patternCompare(&zPattern[-1],zString,pInfo,esc)==0 ){
          SQLITE_SKIP_UTF8(zString);
        }
        return *zString!=0;
      }
      while( (c2 = sqlite3Utf8Read(&zString))!=0 ){
        if( noCase ){
          GlogUpperToLower(c2);
          GlogUpperToLower(c);
          while( c2 != 0 && c2 != c ){
            c2 = sqlite3Utf8Read(&zString);
            GlogUpperToLower(c2);
          }
        }else{
          while( c2 != 0 && c2 != c ){
            c2 = sqlite3Utf8Read(&zString);
          }
        }
        if( c2==0 ) return 0;
        if( patternCompare(zPattern,zString,pInfo,esc) ) return 1;
      }
      return 0;
    }else if( c==matchOne && !prevEscape ){
      if( sqlite3Utf8Read(&zString)==0 ){
        return 0;
      }
    }else if( c==matchSet ){
      u32 prior_c = 0;
      assert( esc==0 );    /* This only occurs for GLOB, not LIKE */
      seen = 0;
      invert = 0;
      c = sqlite3Utf8Read(&zString);
      if( c==0 ) return 0;
      c2 = sqlite3Utf8Read(&zPattern);
      if( c2=='^' ){
        invert = 1;
        c2 = sqlite3Utf8Read(&zPattern);
      }
      if( c2==']' ){
        if( c==']' ) seen = 1;
        c2 = sqlite3Utf8Read(&zPattern);
      }
      while( c2 && c2!=']' ){
        if( c2=='-' && zPattern[0]!=']' && zPattern[0]!=0 && prior_c>0 ){
          c2 = sqlite3Utf8Read(&zPattern);
          if( c>=prior_c && c<=c2 ) seen = 1;
          prior_c = 0;
        }else{
          if( c==c2 ){
            seen = 1;
          }
          prior_c = c2;
        }
        c2 = sqlite3Utf8Read(&zPattern);
      }
      if( c2==0 || (seen ^ invert)==0 ){
        return 0;
      }
    }else if( esc==c && !prevEscape ){
      prevEscape = 1;
    }else{
      c2 = sqlite3Utf8Read(&zString);
      if( noCase ){
        GlogUpperToLower(c);
        GlogUpperToLower(c2);
      }
      if( c!=c2 ){
        return 0;
      }
      prevEscape = 0;
    }
  }
  return *zString==0;
}

/*
** The sqlite3_strglob() interface.
*/
 int sqlite3_strglob(const char *zGlobPattern, const char *zString){
  return patternCompare((u8*)zGlobPattern, (u8*)zString, &globInfo, 0)==0;
}

/*
** Implementation of the like() SQL function.  This function implements
** the build-in LIKE operator.  The first argument to the function is the
** pattern and the second argument is the string.  So, the SQL statements:
**
**       A LIKE B
**
** is implemented as like(B,A).
**
** This same function (with a different compareInfo structure) computes
** the GLOB operator.
*/
func sql_like(context *Context, args []*sqlite3_value) {
	var escape	rune

	db := Context_db_handle(context)
	zB := args[0].Text()
	zA := args[1].Text()

	/* Limit the length of the LIKE or GLOB pattern to avoid problems of deep recursion and N*N behavior in patternCompare(). */
	nPat := sqlite3_value_bytes(args[0])
	if nPat > db.aLimit[SQLITE_LIMIT_LIKE_PATTERN_LENGTH] {
		sqlite3_result_error(context, "LIKE or GLOB pattern too complex", -1)
		return
	}

	if len(args) == 3 {
		/* The escape character string must consist of a single UTF-8 character.
		** Otherwise, return an error.
		*/
		zEsc := args[2].Text()
		if zEsc == "" {
			return
		}
		if len(zEsc) != 1 {
			sqlite3_result_error(context, "ESCAPE expression must be a single character", -1)
			return
		}
		escape = sqlite3Utf8Read(&zEsc)
	}
	if zA != "" && zB != "" {
		pInfo := sqlite3_user_data(context)
		sqlite3_result_int(context, patternCompare(zB, zA, pInfo, escape))
	}
}

/*
** Implementation of the NULLIF(x,y) function.  The result is the first
** argument if the arguments are different.  The result is NULL if the
** arguments are equal to each other.
*/
void nullifFunc(
  Context *context,
  int NotUsed,
  sqlite3_value **argv
){
  CollationSequence *pColl = sqlite3GetFuncCollSeq(context);
  UNUSED_PARAMETER(NotUsed);
  if( sqlite3MemCompare(argv[0], argv[1], pColl)!=0 ){
    sqlite3_result_value(context, argv[0]);
  }
}

/*
** Implementation of the sqlite_version() function.  The result is the version
** of the SQLite library that is running.
*/
void versionFunc(
  Context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  /* IMP: R-48699-48617 This function is an SQL wrapper around the
  ** sqlite3_libversion() C-interface. */
  sqlite3_result_text(context, sqlite3_libversion(), -1, SQLITE_STATIC);
}

/*
** Implementation of the sqlite_source_id() function. The result is a string
** that identifies the particular version of the source code used to build
** SQLite.
*/
void sourceidFunc(
  Context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  /* IMP: R-24470-31136 This function is an SQL wrapper around the
  ** sqlite3_sourceid() C interface. */
  sqlite3_result_text(context, sqlite3_sourceid(), -1, SQLITE_STATIC);
}

/*
** Implementation of the sqlite_log() function.  This is a wrapper around
** sqlite3_log().  The return value is NULL.  The function exists purely for
** its side-effects.
*/
void errlogFunc(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  UNUSED_PARAMETER(argc);
  UNUSED_PARAMETER(context);
  sqlite3_log(sqlite3_value_int(argv[0]), "%s", argv[1].Text());
}

/*
** Implementation of the sqlite_compileoption_used() function.
** The result is an integer that identifies if the compiler option
** was used to build SQLite.
*/
#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
void compileoptionusedFunc(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  /* IMP: R-39564-36305 The sqlite_compileoption_used() SQL
  ** function is a wrapper around the sqlite3_compileoption_used() C/C++
  ** function.
  */
  if zOptName := argv[0].Text(); zOptName != "" {
    sqlite3_result_int(context, sqlite3_compileoption_used(zOptName));
  }
}
#endif /* SQLITE_OMIT_COMPILEOPTION_DIAGS */

/*
** Implementation of the sqlite_compileoption_get() function. 
** The result is a string that identifies the compiler options 
** used to build SQLite.
*/
#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
void compileoptiongetFunc(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  int n;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  /* IMP: R-04922-24076 The sqlite_compileoption_get() SQL function
  ** is a wrapper around the sqlite3_compileoption_get() C/C++ function.
  */
  n = sqlite3_value_int(argv[0]);
  sqlite3_result_text(context, sqlite3_compileoption_get(n), -1, SQLITE_STATIC);
}
#endif /* SQLITE_OMIT_COMPILEOPTION_DIAGS */

/* Array for converting from half-bytes (nybbles) into ASCII hex
** digits. */
const char hexdigits[] = {
  '0', '1', '2', '3', '4', '5', '6', '7',
  '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' 
};

/*
** EXPERIMENTAL - This is not an official function.  The interface may
** change.  This function may disappear.  Do not write code that depends
** on this function.
**
** Implementation of the QUOTE() function.  This function takes a single
** argument.  If the argument is numeric, the return value is the same as
** the argument.  If the argument is NULL, the return value is the string
** "NULL".  Otherwise, the argument is enclosed in single quotes with
** single-quote escapes.
*/
void quoteFunc(Context *context, int argc, sqlite3_value **argv){
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  switch( sqlite3_value_type(argv[0]) ){
    case SQLITE_FLOAT: {
      float64 r1, r2;
      char zBuf[50];
      r1 = sqlite3_value_float64(argv[0]);
      sqlite3_snprintf(sizeof(zBuf), zBuf, "%!.15g", r1);
      sqlite3AtoF(zBuf, &r2, 20)
      if( r1!=r2 ){
        sqlite3_snprintf(sizeof(zBuf), zBuf, "%!.20e", r1);
      }
      sqlite3_result_text(context, zBuf, -1, SQLITE_TRANSIENT);
      break;
    }
    case SQLITE_INTEGER: {
      sqlite3_result_value(context, argv[0]);
      break;
    }
    case SQLITE_BLOB: {
      char *zText = 0;
      char const *zBlob = sqlite3_value_blob(argv[0]);
      int nBlob = sqlite3_value_bytes(argv[0]);
      assert( zBlob==sqlite3_value_blob(argv[0]) )
      zText = (char *)contextMalloc(context, (2*(i64)nBlob)+4); 
      if( zText ){
        int i;
        for(i=0; i<nBlob; i++){
          zText[(i*2)+2] = hexdigits[(zBlob[i]>>4)&0x0F];
          zText[(i*2)+3] = hexdigits[(zBlob[i])&0x0F];
        }
        zText[(nBlob*2)+2] = '\'';
        zText[(nBlob*2)+3] = '\0';
        zText[0] = 'X';
        zText[1] = '\'';
        sqlite3_result_text(context, zText, -1, SQLITE_TRANSIENT);
        sqlite3_free(zText);
      }
      break;
    }
    case SQLITE_TEXT: {
      int i,j;
      u64 n;
      zArg := argv[0].Text()
      char *z;

      if( zArg==0 ) return;
      for(i=0, n=0; zArg[i]; i++){ if( zArg[i]=='\'' ) n++; }
      z = contextMalloc(context, ((i64)i)+((i64)n)+3);
      if( z ){
        z[0] = '\'';
        for(i=0, j=1; zArg[i]; i++){
          z[j++] = zArg[i];
          if( zArg[i]=='\'' ){
            z[j++] = '\'';
          }
        }
        z[j++] = '\'';
        z[j] = 0;
        sqlite3_result_text(context, z, j, sqlite3_free);
      }
      break;
    }
    default: {
      assert( sqlite3_value_type(argv[0])==SQLITE_NULL );
      sqlite3_result_text(context, "NULL", 4, SQLITE_STATIC);
      break;
    }
  }
}

/*
** The unicode() function.  Return the integer unicode code-point value
** for the first character of the input string. 
*/
void unicodeFunc(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  z := argv[0].Text()
  (void)argc;
  if( z && z[0] ) sqlite3_result_int(context, sqlite3Utf8Read(&z));
}

/*
** The char() function takes zero or more arguments, each of which is
** an integer.  It constructs a string where each character of the string
** is the unicode character for the corresponding integer argument.
*/
void charFunc(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  unsigned char *z, *zOut;
  int i;
  zOut = z = sqlite3_malloc( argc*4 );
  if( z==0 ){
    sqlite3_result_error_nomem(context);
    return;
  }
  for(i=0; i<argc; i++){
    sqlite3_int64 x;
    unsigned c;
    x = sqlite3_value_int64(argv[i]);
    if( x<0 || x>0x10ffff ) x = 0xfffd;
    c = (unsigned)(x & 0x1fffff);
    if( c<0x00080 ){
      *zOut++ = (u8)(c&0xFF);
    }else if( c<0x00800 ){
      *zOut++ = 0xC0 + (u8)((c>>6)&0x1F);
      *zOut++ = 0x80 + (u8)(c & 0x3F);
    }else if( c<0x10000 ){
      *zOut++ = 0xE0 + (u8)((c>>12)&0x0F);
      *zOut++ = 0x80 + (u8)((c>>6) & 0x3F);
      *zOut++ = 0x80 + (u8)(c & 0x3F);
    }else{
      *zOut++ = 0xF0 + (u8)((c>>18) & 0x07);
      *zOut++ = 0x80 + (u8)((c>>12) & 0x3F);
      *zOut++ = 0x80 + (u8)((c>>6) & 0x3F);
      *zOut++ = 0x80 + (u8)(c & 0x3F);
    }                                                    \
  }
  sqlite3_result_text(context, (char*)z, (int)(zOut-z), sqlite3_free);
}

/*
** The hex() function.  Interpret the argument as a blob.  Return
** a hexadecimal rendering as text.
*/
void hexFunc(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  int i, n;
  const unsigned char *pBlob;
  char *zHex, *z;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  pBlob = sqlite3_value_blob(argv[0]);
  n = sqlite3_value_bytes(argv[0]);
  assert( pBlob==sqlite3_value_blob(argv[0]) )
  z = zHex = contextMalloc(context, ((i64)n)*2 + 1);
  if( zHex ){
    for(i=0; i<n; i++, pBlob++){
      unsigned char c = *pBlob;
      *(z++) = hexdigits[(c>>4)&0xf];
      *(z++) = hexdigits[c&0xf];
    }
    *z = 0;
    sqlite3_result_text(context, zHex, n*2, sqlite3_free);
  }
}

/*
** The zeroblob(N) function returns a zero-filled blob of size N bytes.
*/
void zeroblobFunc(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  i64 n;
  sqlite3 *db = Context_db_handle(context);
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  n = sqlite3_value_int64(argv[0]);
  if( n>db->aLimit[SQLITE_LIMIT_LENGTH] ){
    sqlite3_result_error_toobig(context);
  }else{
    sqlite3_result_zeroblob(context, (int)n); /* IMP: R-00293-64994 */
  }
}

/*
** The replace() function.  Three arguments are all strings: call
** them A, B, and C. The result is also a string which is derived
** from A by replacing every occurance of B with C.  The match
** must be exact.  Collating sequences are not used.
*/
void replaceFunc(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zStr;        /* The input string A */
  const unsigned char *zPattern;    /* The pattern string B */
  const unsigned char *zRep;        /* The replacement string C */
  unsigned char *zOut;              /* The output */
  int nStr;                /* Size of zStr */
  int nPattern;            /* Size of zPattern */
  int nRep;                /* Size of zRep */
  i64 nOut;                /* Maximum size of zOut */
  int loopLimit;           /* Last zStr[] that might match zPattern[] */
  int i, j;                /* Loop counters */

  assert( argc==3 );
  UNUSED_PARAMETER(argc);
  zStr = argv[0].Text()
  if( zStr==0 ) return;
  nStr = sqlite3_value_bytes(argv[0]);
  assert( zStr == argv[0].Text() )
  zPattern = argv[1].Text()
  if( zPattern==0 ){
    assert( sqlite3_value_type(argv[1])==SQLITE_NULL
            || Context_db_handle(context)->mallocFailed );
    return;
  }
  if( zPattern[0]==0 ){
    assert( sqlite3_value_type(argv[1])!=SQLITE_NULL );
    sqlite3_result_value(context, argv[0]);
    return;
  }
  nPattern = sqlite3_value_bytes(argv[1]);
  assert( zPattern == argv[1].Text() )
  zRep = argv[2].Text()
  if( zRep==0 ) return;
  nRep = sqlite3_value_bytes(argv[2]);
  assert( zRep == argv[2].Text() )
  nOut = nStr + 1;
  assert( nOut<SQLITE_MAX_LENGTH );
  zOut = contextMalloc(context, (i64)nOut);
  if( zOut==0 ){
    return;
  }
  loopLimit = nStr - nPattern;  
  for(i=j=0; i<=loopLimit; i++){
    if( zStr[i]!=zPattern[0] || memcmp(&zStr[i], zPattern, nPattern) ){
      zOut[j++] = zStr[i];
    }else{
      u8 *zOld;
      sqlite3 *db = Context_db_handle(context);
      nOut += nRep - nPattern;
      if( nOut-1>db->aLimit[SQLITE_LIMIT_LENGTH] ){
        sqlite3_result_error_toobig(context);
        sqlite3_free(zOut);
        return;
      }
      zOld = zOut;
      zOut = sqlite3_realloc(zOut, (int)nOut);
      if( zOut==0 ){
        sqlite3_result_error_nomem(context);
        sqlite3_free(zOld);
        return;
      }
      memcpy(&zOut[j], zRep, nRep);
      j += nRep;
      i += nPattern-1;
    }
  }
  assert( j+nStr-i+1==nOut );
  memcpy(&zOut[j], &zStr[i], nStr-i);
  j += nStr - i;
  assert( j<=nOut );
  zOut[j] = 0;
  sqlite3_result_text(context, (char*)zOut, j, sqlite3_free);
}

/*
** Implementation of the TRIM(), LTRIM(), and RTRIM() functions.
** The userdata is 0x1 for left trim, 0x2 for right trim, 0x3 for both.
*/
void trimFunc(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zIn;         /* Input string */
  const unsigned char *zCharSet;    /* Set of characters to trim */
  int nIn;                          /* Number of bytes in input */
  int flags;                        /* 1: trimleft  2: trimright  3: trim */
  int i;                            /* Loop counter */
  unsigned char *aLen = 0;          /* Length of each character in zCharSet */
  unsigned char **azChar = 0;       /* Individual characters in zCharSet */
  int nChar;                        /* Number of characters in zCharSet */

  if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
    return;
  }
  zIn = argv[0].Text()
  if( zIn==0 ) return;
  nIn = sqlite3_value_bytes(argv[0]);
  assert( zIn == argv[0].Text() );
  if( argc==1 ){
    const unsigned char lenOne[] = { 1 };
    unsigned char * const azOne[] = { (u8*)" " };
    nChar = 1;
    aLen = (u8*)lenOne;
    azChar = (unsigned char **)azOne;
    zCharSet = 0;
  }else if (zCharSet = argv[1].Text()) == "" {
    return;
  }else{
    const unsigned char *z;
    for(z=zCharSet, nChar=0; *z; nChar++){
      SQLITE_SKIP_UTF8(z);
    }
    if( nChar>0 ){
      azChar = contextMalloc(context, ((i64)nChar)*(sizeof(char*)+1));
      if( azChar==0 ){
        return;
      }
      aLen = (unsigned char*)&azChar[nChar];
      for(z=zCharSet, nChar=0; *z; nChar++){
        azChar[nChar] = (unsigned char *)z;
        SQLITE_SKIP_UTF8(z);
        aLen[nChar] = (u8)(z - azChar[nChar]);
      }
    }
  }
  if( nChar>0 ){
    flags = SQLITE_PTR_TO_INT(sqlite3_user_data(context));
    if( flags & 1 ){
      while( nIn>0 ){
        int len = 0;
        for(i=0; i<nChar; i++){
          len = aLen[i];
          if( len<=nIn && memcmp(zIn, azChar[i], len)==0 ) break;
        }
        if( i>=nChar ) break;
        zIn += len;
        nIn -= len;
      }
    }
    if( flags & 2 ){
      while( nIn>0 ){
        int len = 0;
        for(i=0; i<nChar; i++){
          len = aLen[i];
          if( len<=nIn && memcmp(&zIn[nIn-len],azChar[i],len)==0 ) break;
        }
        if( i>=nChar ) break;
        nIn -= len;
      }
    }
    if( zCharSet ){
      sqlite3_free(azChar);
    }
  }
  sqlite3_result_text(context, (char*)zIn, nIn, SQLITE_TRANSIENT);
}


/* IMP: R-25361-16150 This function is omitted from SQLite by default. It
** is only available if the SQLITE_SOUNDEX compile-time option is used
** when SQLite is built.
*/
#ifdef SQLITE_SOUNDEX
/*
** Compute the soundex encoding of a word.
**
** IMP: R-59782-00072 The soundex(X) function returns a string that is the
** soundex encoding of the string X. 
*/
void soundexFunc(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  char zResult[8];
  const u8 *zIn;
  int i, j;
  const unsigned char iCode[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 1, 2, 3, 0, 1, 2, 0, 0, 2, 2, 4, 5, 5, 0,
    1, 2, 6, 2, 3, 0, 1, 0, 2, 0, 2, 0, 0, 0, 0, 0,
    0, 0, 1, 2, 3, 0, 1, 2, 0, 0, 2, 2, 4, 5, 5, 0,
    1, 2, 6, 2, 3, 0, 1, 0, 2, 0, 2, 0, 0, 0, 0, 0,
  };
  assert( argc==1 );
  zIn = ([]byte)(argv[0].Text())
  if( zIn==0 ) zIn = (u8*)"";
  for(i=0; zIn[i] && !sqlite3Isalpha(zIn[i]); i++){}
  if( zIn[i] ){
    u8 prevcode = iCode[zIn[i]&0x7f];
    zResult[0] = strings.ToUpper(zIn[i:])[0]
    for(j=1; j<4 && zIn[i]; i++){
      int code = iCode[zIn[i]&0x7f];
      if( code>0 ){
        if( code!=prevcode ){
          prevcode = code;
          zResult[j++] = code + '0';
        }
      }else{
        prevcode = 0;
      }
    }
    while( j<4 ){
      zResult[j++] = '0';
    }
    zResult[j] = 0;
    sqlite3_result_text(context, zResult, 4, SQLITE_TRANSIENT);
  }else{
    /* IMP: R-64894-50321 The string "?000" is returned if the argument
    ** is NULL or contains no ASCII alphabetic characters. */
    sqlite3_result_text(context, "?000", 4, SQLITE_STATIC);
  }
}
#endif /* SQLITE_SOUNDEX */

#ifndef SQLITE_OMIT_LOAD_EXTENSION
/*
** A function that loads a shared-library extension then returns NULL.
*/
void loadExt(Context *context, int argc, sqlite3_value **argv){
  zFile := argv[0].Text()
  const char *zProc;
  sqlite3 *db = Context_db_handle(context);
  char *zErrMsg = 0;

  if( argc==2 ){
    zProc = argv[1].Text()
  }else{
    zProc = 0;
  }
  if( zFile && sqlite3_load_extension(db, zFile, zProc, &zErrMsg) ){
    sqlite3_result_error(context, zErrMsg, -1);
    sqlite3_free(zErrMsg);
  }
}
#endif


/*
** An instance of the following structure holds the context of a
** sum() or avg() aggregate computation.
*/
typedef struct SumCtx SumCtx;
struct SumCtx {
  float64 rSum;      /* Floating point sum */
  i64 iSum;         /* Integer sum */   
  i64 cnt;          /* Number of elements summed */
  u8 overflow;      /* True if integer overflow seen */
  u8 approx;        /* True if non-integer value was input to the sum */
};

/*
** Routines used to compute the sum, average, and total.
**
** The SUM() function follows the (broken) SQL standard which means
** that it returns NULL if it sums over no inputs.  TOTAL returns
** 0.0 in that case.  In addition, TOTAL always returns a float where
** SUM might return an integer if it never encounters a floating point
** value.  TOTAL never fails, but SUM might through an exception if
** it overflows an integer.
*/
void sumStep(Context *context, int argc, sqlite3_value **argv){
  SumCtx *p;
  int type;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  p = sqlite3_aggregate_context(context, sizeof(*p));
  type = sqlite3_value_numeric_type(argv[0]);
  if( p && type!=SQLITE_NULL ){
    p->cnt++;
    if( type==SQLITE_INTEGER ){
      i64 v = sqlite3_value_int64(argv[0]);
      p->rSum += v;
      if( (p->approx|p->overflow)==0 && sqlite3AddInt64(&p->iSum, v) ){
        p->overflow = 1;
      }
    }else{
      p->rSum += sqlite3_value_float64(argv[0]);
      p->approx = 1;
    }
  }
}
void sumFinalize(Context *context){
  SumCtx *p;
  p = sqlite3_aggregate_context(context, 0);
  if( p && p->cnt>0 ){
    if( p->overflow ){
      sqlite3_result_error(context,"integer overflow",-1);
    }else if( p->approx ){
      sqlite3_result_float64(context, p->rSum);
    }else{
      sqlite3_result_int64(context, p->iSum);
    }
  }
}
void avgFinalize(Context *context){
  SumCtx *p;
  p = sqlite3_aggregate_context(context, 0);
  if( p && p->cnt>0 ){
    sqlite3_result_float64(context, p->rSum/(float64)p->cnt);
  }
}
void totalFinalize(Context *context){
  SumCtx *p;
  p = sqlite3_aggregate_context(context, 0);
  sqlite3_result_float64(context, p ? p->rSum : 0);
}

/*
** The following structure keeps track of state information for the
** count() aggregate function.
*/
typedef struct CountCtx CountCtx;
struct CountCtx {
  i64 n;
};

/*
** Routines to implement the count() aggregate function.
*/
void countStep(Context *context, int argc, sqlite3_value **argv){
  CountCtx *p;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  if( (argc==0 || SQLITE_NULL!=sqlite3_value_type(argv[0])) && p ){
    p->n++;
  }
}   
void countFinalize(Context *context){
  CountCtx *p;
  p = sqlite3_aggregate_context(context, 0);
  sqlite3_result_int64(context, p ? p->n : 0);
}

/*
** Routines to implement min() and max() aggregate functions.
*/
void minmaxStep(
  Context *context, 
  int NotUsed, 
  sqlite3_value **argv
){
  Mem *pArg  = (Mem *)argv[0];
  Mem *pBest;
  UNUSED_PARAMETER(NotUsed);

  pBest = (Mem *)sqlite3_aggregate_context(context, sizeof(*pBest));
  if( !pBest ) return;

  if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
    if( pBest->flags ) sqlite3SkipAccumulatorLoad(context);
  }else if( pBest->flags ){
    int max;
    int cmp;
    CollationSequence *pColl = sqlite3GetFuncCollSeq(context);
    /* This step function is used for both the min() and max() aggregates,
    ** the only difference between the two being that the sense of the
    ** comparison is inverted. For the max() aggregate, the
    ** sqlite3_user_data() function returns (void *)-1. For min() it
    ** returns (void *)db, where db is the sqlite3* database pointer.
    ** Therefore the next statement sets variable 'max' to 1 for the max()
    ** aggregate, or 0 for min().
    */
    max = sqlite3_user_data(context)!=0;
    cmp = sqlite3MemCompare(pBest, pArg, pColl);
    if( (max && cmp<0) || (!max && cmp>0) ){
      sqlite3VdbeMemCopy(pBest, pArg);
    }else{
      sqlite3SkipAccumulatorLoad(context);
    }
  }else{
    sqlite3VdbeMemCopy(pBest, pArg);
  }
}
void minMaxFinalize(Context *context){
  sqlite3_value *pRes;
  pRes = (sqlite3_value *)sqlite3_aggregate_context(context, 0);
  if( pRes ){
    if( pRes->flags ){
      sqlite3_result_value(context, pRes);
    }
    sqlite3VdbeMemRelease(pRes);
  }
}

/*
** group_concat(EXPR, ?SEPARATOR?)
*/
void groupConcatStep(
  Context *context,
  int argc,
  sqlite3_value **argv
){
  const char *zVal;
  StrAccum *pAccum;
  const char *zSep;
  int nVal, nSep;
  assert( argc==1 || argc==2 );
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ) return;
  pAccum = (StrAccum*)sqlite3_aggregate_context(context, sizeof(*pAccum));

  if( pAccum ){
    sqlite3 *db = Context_db_handle(context);
    int firstTerm = pAccum->useMalloc==0;
    pAccum->useMalloc = 2;
    pAccum->mxAlloc = db->aLimit[SQLITE_LIMIT_LENGTH];
    if( !firstTerm ){
      if( argc==2 ){
        zSep = argv[1].Text()
        nSep = sqlite3_value_bytes(argv[1]);
      }else{
        zSep = ",";
        nSep = 1;
      }
      sqlite3StrAccumAppend(pAccum, zSep, nSep);
    }
    zVal = argv[0].Text()
    nVal = sqlite3_value_bytes(argv[0]);
    sqlite3StrAccumAppend(pAccum, zVal, nVal);
  }
}
void groupConcatFinalize(Context *context){
  StrAccum *pAccum;
  pAccum = sqlite3_aggregate_context(context, 0);
  if( pAccum ){
    if( pAccum->tooBig ){
      sqlite3_result_error_toobig(context);
    }else if( pAccum->mallocFailed ){
      sqlite3_result_error_nomem(context);
    }else{    
      sqlite3_result_text(context, sqlite3StrAccumFinish(pAccum), -1, 
                          sqlite3_free);
    }
  }
}

/*
** This routine does per-connection function registration.  Most
** of the built-in functions above are part of the global function set.
** This routine only deals with those that are not global.
*/
func (db *sqlite3) RegisterBuiltinFunctions() {
	rc := db.OverloadFunction("MATCH", 2)
	assert( rc == SQLITE_NOMEM || rc == SQLITE_OK )
	if rc == SQLITE_NOMEM {
		db.mallocFailed = true
	}
}

/*
** Set the LIKEOPT flag on the 2-argument function with the given name.
*/
void setLikeOptFlag(sqlite3 *db, const char *zName, u8 flagVal){
  FuncDef *pDef;
  pDef = db.FindFunction(zName, 2, false)
  if( ALWAYS(pDef) ){
    pDef->flags = flagVal;
  }
}

/*
** Register the built-in LIKE and GLOB functions.  The caseSensitive
** parameter determines whether or not the LIKE operator is case
** sensitive.  GLOB is always case sensitive.
*/
func (db *sqlite3) RegisterLikeFunctions(caseSensitive bool) {
	struct compareInfo *pInfo;
	if caseSensitive {
		pInfo = (struct compareInfo*)&likeInfoAlt
	} else {
		pInfo = (struct compareInfo*)&likeInfoNorm
	}
	db.CreateFunc("like", 2, pInfo, sql_like, nil, nil, nil)
	db.CreateFunc("like", 3, pInfo, sql_like, nil, nil, nil)
	db.CreateFunc("glob", 2, (struct compareInfo*)&globInfo, sql_like, nil, nil, nil)
	setLikeOptFlag(db, "glob", SQLITE_FUNC_LIKE | SQLITE_FUNC_CASE);
	if caseSensitive {
		setLikeOptFlag(db, "like", SQLITE_FUNC_LIKE | SQLITE_FUNC_CASE)
	} else {
		setLikeOptFlag(db, "like", SQLITE_FUNC_LIKE)
	}
}

/*
** pExpr points to an expression which implements a function.  If
** it is appropriate to apply the LIKE optimization to that function
** then set aWc[0] through aWc[2] to the wildcard characters and
** return TRUE.  If the function is not a LIKE-style function then
** return FALSE.
*/
int sqlite3IsLikeFunction(sqlite3 *db, Expr *pExpr, int *pIsNocase, char *aWc){
  FuncDef *pDef;
  if( pExpr->op!=TK_FUNCTION 
   || !pExpr->x.pList 
   || pExpr->x.pList->nExpr!=2
  ){
    return 0;
  }
  assert( !ExprHasProperty(pExpr, EP_xIsSelect) );
  pDef = db.FindFunction(pExpr->u.zToken, 2, false)
  if( NEVER(pDef==0) || (pDef->flags & SQLITE_FUNC_LIKE)==0 ){
    return 0;
  }

  /* The memcpy() statement assumes that the wildcard characters are
  ** the first three statements in the compareInfo structure.  The
  ** asserts() that follow verify that assumption
  */
  memcpy(aWc, pDef.UserData, 3)
  assert( (char*)&likeInfoAlt == (char*)&likeInfoAlt.matchAll );
  assert( &((char*)&likeInfoAlt)[1] == (char*)&likeInfoAlt.matchOne );
  assert( &((char*)&likeInfoAlt)[2] == (char*)&likeInfoAlt.matchSet );
  *pIsNocase = (pDef->flags & SQLITE_FUNC_CASE)==0;
  return 1;
}

/*
** All all of the FuncDef structures in the BuiltinFunctions[] array above
** to the global function hash table.  This occurs at start-time (as
** a consequence of calling sqlite3_initialize()).
**
** After this routine runs
*/
void sqlite3RegisterGlobalFunctions(void){
	/*
	** The following array holds FuncDef structures for all of the functions
	** defined in this file.
	**
	** The array cannot be constant since changes are made to the
	** FuncDef.pHash elements at start-time.  The elements of this array
	** are read-only after initialization is complete.
	*/
	var BuiltinFunctions []Function = {
		FUNCTION(ltrim,              1, 1, 0, trimFunc         ),
		FUNCTION(ltrim,              2, 1, 0, trimFunc         ),
		FUNCTION(rtrim,              1, 2, 0, trimFunc         ),
		FUNCTION(rtrim,              2, 2, 0, trimFunc         ),
		FUNCTION(trim,               1, 3, 0, trimFunc         ),
		FUNCTION(trim,               2, 3, 0, trimFunc         ),
		FUNCTION(min,               -1, 0, 1, minmaxFunc       ),
		FUNCTION(min,                0, 0, 1, 0                ),
		AGGREGATE(min,               1, 0, 1, minmaxStep,      minMaxFinalize ),
		FUNCTION(max,               -1, 1, 1, minmaxFunc       ),
		FUNCTION(max,                0, 1, 1, 0                ),
		AGGREGATE(max,               1, 1, 1, minmaxStep,      minMaxFinalize ),
		FUNCTION2(typeof,            1, 0, 0, typeofFunc,  SQLITE_FUNC_TYPEOF),
		FUNCTION2(length,            1, 0, 0, lengthFunc,  SQLITE_FUNC_LENGTH),
		FUNCTION(instr,              2, 0, 0, instrFunc        ),
		FUNCTION(substr,             2, 0, 0, substrFunc       ),
		FUNCTION(substr,             3, 0, 0, substrFunc       ),
		FUNCTION(unicode,            1, 0, 0, unicodeFunc      ),
		FUNCTION(char,              -1, 0, 0, charFunc         ),
		FUNCTION(abs,                1, 0, 0, absFunc          ),
		FUNCTION(round,              1, 0, 0, roundFunc        ),
		FUNCTION(round,              2, 0, 0, roundFunc        ),
		FUNCTION(upper,              1, 0, 0, upperFunc        ),
		FUNCTION(lower,              1, 0, 0, lowerFunc        ),
		FUNCTION(coalesce,           1, 0, 0, 0                ),
		FUNCTION(coalesce,           0, 0, 0, 0                ),
		FUNCTION2(coalesce,         -1, 0, 0, ifnullFunc,  SQLITE_FUNC_COALESCE),
		FUNCTION(hex,                1, 0, 0, hexFunc          ),
		FUNCTION2(ifnull,            2, 0, 0, ifnullFunc,  SQLITE_FUNC_COALESCE),
		FUNCTION(random,             0, 0, 0, randomFunc       ),
		FUNCTION(randomblob,         1, 0, 0, randomBlob       ),
		FUNCTION(nullif,             2, 0, 1, nullifFunc       ),
		FUNCTION(sqlite_version,     0, 0, 0, versionFunc      ),
		FUNCTION(sqlite_source_id,   0, 0, 0, sourceidFunc     ),
		FUNCTION(sqlite_log,         2, 0, 0, errlogFunc       ),
#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
		FUNCTION(sqlite_compileoption_used,1, 0, 0, compileoptionusedFunc  ),
		FUNCTION(sqlite_compileoption_get, 1, 0, 0, compileoptiongetFunc  ),
#endif /* SQLITE_OMIT_COMPILEOPTION_DIAGS */
		FUNCTION(quote,              1, 0, 0, quoteFunc        ),
		FUNCTION(last_insert_rowid,  0, 0, 0, last_insert_rowid),
		FUNCTION(changes,            0, 0, 0, changes          ),
		FUNCTION(total_changes,      0, 0, 0, total_changes    ),
		FUNCTION(replace,            3, 0, 0, replaceFunc      ),
		FUNCTION(zeroblob,           1, 0, 0, zeroblobFunc     ),
 #ifdef SQLITE_SOUNDEX
 		FUNCTION(soundex,            1, 0, 0, soundexFunc      ),
 #endif
 #ifndef SQLITE_OMIT_LOAD_EXTENSION
		FUNCTION(load_extension,     1, 0, 0, loadExt          ),
		FUNCTION(load_extension,     2, 0, 0, loadExt          ),
 #endif
		AGGREGATE(sum,               1, 0, 0, sumStep,         sumFinalize    ),
		AGGREGATE(total,             1, 0, 0, sumStep,         totalFinalize    ),
		AGGREGATE(avg,               1, 0, 0, sumStep,         avgFinalize    ),
 /*		AGGREGATE(count,             0, 0, 0, countStep,       countFinalize  ), */
		{0, SQLITE_FUNC_COUNT, nil, nil, nil, countStep, countFinalize, "count", nil, nil},
		AGGREGATE(count,             1, 0, 0, countStep,       countFinalize  ),
		AGGREGATE(group_concat,      1, 0, 0, groupConcatStep, groupConcatFinalize),
		AGGREGATE(group_concat,      2, 0, 0, groupConcatStep, groupConcatFinalize),
  
		LIKEFUNC(glob, 2, &globInfo, SQLITE_FUNC_LIKE|SQLITE_FUNC_CASE),
#ifdef SQLITE_CASE_SENSITIVE_LIKE
		LIKEFUNC(like, 2, &likeInfoAlt, SQLITE_FUNC_LIKE|SQLITE_FUNC_CASE),
		LIKEFUNC(like, 3, &likeInfoAlt, SQLITE_FUNC_LIKE|SQLITE_FUNC_CASE),
#else
		LIKEFUNC(like, 2, &likeInfoNorm, SQLITE_FUNC_LIKE),
		LIKEFUNC(like, 3, &likeInfoNorm, SQLITE_FUNC_LIKE),
#endif
}

	for i, v := range BuiltinFunctions {
		sqlite3FuncDefInsert(sqlite3GlobalFunctions, v)
	}
	sqlite3RegisterDateTimeFunctions()
#ifndef SQLITE_OMIT_ALTERTABLE
	sqlite3AlterFunctions()
#endif
}