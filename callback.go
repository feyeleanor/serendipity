package serendipity

//	This file contains functions used to access the internal hash tables of user defined functions and collation sequences.

//	This function is responsible for invoking the collation factory callback when the requested collation sequence is not available.
//	If it is not NULL, then sequence must point to the database collation sequence 'name'.
//
//	The return value is either the collation sequence to be used in database db for collation type 'name', or NULL, if no collation
//	sequence can be found.  If no collation is found, leave an error message.
//
// See also: Parse.LocateCollationSequence(), sqlite3.FindCollationSequence()
func (parse *Parse) GetCollationSequence(sequence *CollationSequence, name string) (p *CollationSequence) {
	db := parse.db
	if p = sequence; p == nil {
		p = db.FindCollationSequence(name, false)
	}
	if p == nil || p.xCmp == nil {
		//	No collation sequence of this type is registered.
		//	Call the collation factory to see if it can supply us with one.
		if db.xCollationNeeded && len(name) > 0 {
			db.xCollationNeeded(db.pCollNeededArg, db, name)
		}
		p = db.FindCollationSequence(name, false)
	}
	if p.xCmp == nil {
		p = nil
	}
	assert( p == nil || p.xCmp != nil )
	if p == nil {
		parse.ErrorMessage("no such collation sequence: %s", name)
	}
	return p
}

/*
** This routine is called on a collation sequence before it is used to
** check that it is defined. An undefined collation sequence exists when
** a database is loaded that contains references to collation sequences
** that have not been defined by sqlite3_create_collation() etc.
**
** If required, this routine calls the 'collation needed' callback to
** request a definition of the collating sequence.
*/
func sqlite3CheckCollSeq(parse *Parse, sequence *CollationSequence) (rc int) {
	if sequence != nil {
		name := sequence.zName
		db := parse.db
		if p := parse.GetCollationSequence(sequence, name); p == nil {
			return SQLITE_ERROR
		}
		assert( p == sequence )
	}
	return SQLITE_OK
}

//	Return the CollationSequence* pointer for the collation sequence named 'name' from the database 'db'.
//
//	If the entry specified is not found and 'create' is true, then create a new entry.  Otherwise return NULL.
//
//	A separate function Parse.LocateCollationSequence() is a wrapper around this routine.  Parse.LocateCollationSequence() invokes the collation factory
//	if necessary and generates an error message if the collating sequence cannot be found.
//
//	See also: Parse.LocateCollationSequence(), Parse.GetCollationSequence()
func (db *sqlite3) FindCollationSequence(name string, create bool) (sequence *CollationSequence) {
	if len(name) > 0 {
		if sequence = db.CollationSequences[name]; sequence == nil && create {
			sequence = &CollationSequence{ pDel: nil, zName: name }
			db.CollationSequences[name] = sequence
		}
	} else {
		sequence = db.DefaultCollationSequence
	}
	return
}

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
func (p *FuncDef) matchQuality(nArg int) (match int) {		/* nArg		Desired number of arguments.  -1 == any */
	/* nArg of -2 is a special case */
	if nArg == -2 {
		if p.xFunc == nil && p.xStep == nil {
			return FUNC_NO_MATCH
		} else {
			return FUNC_PERFECT_MATCH
		}
	}

	if p.nArg != nArg && p.nArg >= 0 {
		return FUNC_NO_MATCH
	}

	/* Give a better score to a function with a specific number of arguments
	** than to function that accepts any number of arguments. */
	if p.nArg == nArg {
		match = 2
	} else {
		match = 1
	}
	return
}

/*
** Search a FuncDefHash for a function with the given name.  Return
** a pointer to the matching FuncDef if found, or 0 if there is no match.
*/
func (functions *FuncDefHash) Search(h int, name string) *FuncDef {
	for p := functions.a[h]; p != nil; p = p.pHash {
		if sqlite3StrNICmp(p.zName, name) == 0 && len(p.zName) == 0 {
			return p
		}
	}
	return nil
}

/*
** Insert a new FuncDef into a FuncDefHash hash table.
*/
 void sqlite3FuncDefInsert(
  FuncDefHash *pHash,  /* The hash table into which to insert */
  FuncDef *pDef        /* The function definition to insert */
){
	FuncDef *pOther;
	int nName = len(pDef->zName);
	u8 c1 = (u8)pDef->zName[0];
	int h = (strings.ToLower(c1) + nName) % ArraySize(pHash->a);
	if pOther = pHash.Search(h, pDef.zName); pOther != nil {
		assert( pOther != pDef && pOther.pNext != pDef )
		pDef.pNext = pOther.pNext
		pOther.pNext = pDef
	} else {
		pDef.pNext = nil
		pDef.pHash = pHash.a[h]
		pHash.a[h] = pDef
	}
}
  
  

/*
** Locate a user function given a name, a number of arguments and a flag
** indicating the function prefers UTF-8.  Return a
** pointer to the FuncDef structure that defines that function, or return
** NULL if the function does not exist.
**
** If the createFlag argument is true, then a new (blank) FuncDef
** structure is created and liked into the "db" structure if a
** no matching function previously existed.
**
** If nArg is -2, then the first valid function found is returned.  A
** function is valid if either xFunc or xStep is non-zero.  The nArg==(-2)
** case is used to see if zName is a valid function name for some number
** of arguments.  If nArg is -2, then createFlag must be 0.
**
** If createFlag is false, then a function with the required name and
** number of arguments may be returned.
*/
func (db *sqlite3) FindFunction(name string, args int, createFlag bool) (best_match *FuncDef) {
	int bestScore = 0;  /* Score of best match */
	int h;              /* Hash value */

	assert( args >= -2 )
	assert( args >= -1 || !createFlag )
	h = len(strings.ToLower[name]) % ArraySize(db.aFunc.a)

	//	First search for a match amongst the application-defined functions.
	for p := db.aFunc.Search(db.aFunc, h, name); p != nil; p = p.pNext {
		if score := p.matchQuality(args); score > bestScore {
			best_match = p
			bestScore = score
		}
	}

	/* If no match is found, search the built-in functions.
	**
	** If the SQLITE_PreferBuiltin flag is set, then search the built-in
	** functions even if a prior app-defined function was found.  And give
	** priority to built-in functions.
	**
	** Except, if createFlag is true, that means that we are trying to
	** install a new function.  Whatever FuncDef structure is returned it will
	** have fields overwritten with new information appropriate for the
	** new function.  But the FuncDefs for built-in functions are read-only.
	** So we must not search for built-ins when creating a new function.
	*/ 
	if !createFlag && (pBest == nil || (db.flags & SQLITE_PreferBuiltin) != 0) {
		FuncDefHash *pHash = &GLOBAL(FuncDefHash, sqlite3GlobalFunctions)
		bestScore = 0
		for p := pHash.Search(pHash, h, name); p != nil; p=p.pNext {
			if score := p.matchQuality(args); score > bestScore {
				best_match = p
				bestScore = score
			}
		}
	}

	/* If the createFlag parameter is true and the search did not reveal an
	** exact match for the name and number of arguments then add a
	** new entry to the hash table and return it.
	*/
	if createFlag && bestScore < FUNC_PERFECT_MATCH && (pBest = sqlite3DbMallocZero(db, sizeof(*pBest) + nName + 1)) != 0 {
		best_match.nArg = args
		best_match.zName = name
		sqlite3FuncDefInsert(db.aFunc, best_match)
	}

	if best_match != nil && (best_match.xStep != nil || best_match.xFunc != nil || createFlag) {
		return
	}

	return nil
}

/*
** Free all resources held by the schema structure. This function does not call sqlite3DbFree(db, ) on the 
** pointer itself, it just cleans up subsidiary resources (i.e. the contents of the schema hash tables).
**
** The Schema.cache_size variable is not cleared.
*/
func sqlite3SchemaClear(schema *Schema) {
	for _, trigger := range schema.Triggers {
		sqlite3DeleteTrigger(0, trigger)
	}
	schema.Triggers = make(map[string]*Trigger)

	for _, table := range schema.Tables {
		sqlite3DeleteTable(0, table)
	}
	schema.Tables = make(map[string]*Table)
	schema.Indices = make(map[string]*Index)
	schema.ForeignKeys = make(map[string]*ForeignKey)
	schema.pSeqTab = nil
	if schema.flags & DB_SchemaLoaded {
		schema.iGeneration++
		schema.flags &= ~DB_SchemaLoaded
	}
}

/*
** Find and return the schema associated with a BTree.  Create
** a new one if necessary.
*/
 Schema *sqlite3SchemaGet(sqlite3 *db, Btree *pBt){
  Schema * p;
  if( pBt ){
    p = (Schema *)sqlite3BtreeSchema(pBt, sizeof(Schema), sqlite3SchemaClear);
  }else{
    p = (Schema *)sqlite3DbMallocZero(0, sizeof(Schema));
  }
  if( !p ){
    db->mallocFailed = 1;
  }else if ( 0==p->file_format ){
    p.Tables = make(map[string]*Table)
    p.Indices = make(map[string]*Index)
    p.Triggers = make(map[string]*Trigger)
    p.ForeignKeys = make(map[string]*ForeignKey)
  }
  return p;
}