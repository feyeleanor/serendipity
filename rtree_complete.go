package serendipity

import (
	"fmt"
	"math"
)

//	This file contains code for implementations of the r-tree and r*-tree algorithms packaged as an SQLite virtual table module.

//	Database Format of R-Tree Tables
//	--------------------------------
//
//	The data structure for a single virtual r-tree table is stored in three native SQLite tables declared as follows. In each case, the '%' character
//	in the table name is replaced with the user-supplied name of the r-tree table.
//
//		CREATE TABLE %_node(nodeno INTEGER PRIMARY KEY, data BLOB)
//		CREATE TABLE %_parent(nodeno INTEGER PRIMARY KEY, parentnode INTEGER)
//		CREATE TABLE %_rowid(rowid INTEGER PRIMARY KEY, nodeno INTEGER)
//
//	The data for each node of the r-tree structure is stored in the %_node table. For each node that is not the root node of the r-tree, there is
//	an entry in the %_parent table associating the node with its parent. And for each row of data in the table, there is an entry in the %_rowid
//	table that maps from the entries rowid to the id of the node that it is stored on.
//
//	The root node of an r-tree always exists, even if the r-tree table is empty. The nodeno of the root node is always 1. All other nodes in the
//	table must be the same size as the root node. The content of each node is formatted as follows:
//
//		1	If the node is the root node (node 1), then the first 2 bytes of the node contain the tree depth as a big-endian integer.
//			For non-root nodes, the first 2 bytes are left unused.
//
//		2	The next 2 bytes contain the number of entries currently stored in the node.
//
//		3	The remainder of the node contains the node entries. Each entry consists of a single 8-byte integer followed by an even number
//			of 4-byte coordinates. For leaf nodes the integer is the rowid of a record. For internal nodes it is the node number of a child page.


//	This file contains an implementation of a couple of different variants of the r-tree algorithm. See the README file for further details. The 
//	same data-structure is used for all, but the algorithms for insert and delete operations vary. The variants used are selected at compile time 
//	by defining the following symbols:


//	Either, both or none of the following may be set to activate r*tree variant algorithms.

#define VARIANT_RSTARTREE_CHOOSESUBTREE 0
#define VARIANT_RSTARTREE_REINSERT      1

//	Exactly one of the following must be set to 1.
#define VARIANT_GUTTMAN_QUADRATIC_SPLIT 0
#define VARIANT_GUTTMAN_LINEAR_SPLIT    0
#define VARIANT_RSTARTREE_SPLIT         1

#define VARIANT_GUTTMAN_SPLIT (VARIANT_GUTTMAN_LINEAR_SPLIT||VARIANT_GUTTMAN_QUADRATIC_SPLIT)

#if VARIANT_GUTTMAN_QUADRATIC_SPLIT
  #define PickNext QuadraticPickNext
  #define PickSeeds QuadraticPickSeeds
  #define AssignCells splitNodeGuttman
#endif
#if VARIANT_GUTTMAN_LINEAR_SPLIT
  #define PickNext LinearPickNext
  #define PickSeeds LinearPickSeeds
  #define AssignCells splitNodeGuttman
#endif
#if VARIANT_RSTARTREE_SPLIT
  #define AssignCells splitNodeStartree
#endif

#if !defined(NDEBUG) && !defined(SQLITE_DEBUG) 
# define NDEBUG 1
#endif


//	The following macro is used to suppress compiler warnings.
#ifndef UNUSED_PARAMETER
# define UNUSED_PARAMETER(x) (void)(x)
#endif

//	Possible values for eCoordType:
#define RTREE_COORD_REAL32 0
#define RTREE_COORD_INT32  1

//	If SQLITE_RTREE_INT_ONLY is defined, then this virtual table will only deal with integer coordinates.  No floating point operations will be done.
#ifdef SQLITE_RTREE_INT_ONLY
	typedef sqlite3_int64 float64;       /* High accuracy coordinate */
	typedef int RtreeValue;                  /* Low accuracy coordinate */
#else
	typedef float64 float64;              /* High accuracy coordinate */
	typedef float RtreeValue;                /* Low accuracy coordinate */
#endif

//	The minimum number of cells allowed for a node is a third of the maximum. In Gutman's notation:
//
//		m = M/3
//
//	If an R*-tree "Reinsert" operation is required, the same number of cells are removed from the overfull node and reinserted into the tree.
#define RTREE_MINCELLS(p) ((((p).iNodeSize - 4) / (p).nBytesPerCell) / 3)
#define RTREE_REINSERT(p) RTREE_MINCELLS(p)
#define RTREE_MAXCELLS 51

//	The smallest possible node-size is (512-64)==448 bytes. And the largest supported cell size is 48 bytes (8 byte rowid + ten 4 byte coordinates).
//	Therefore all non-root nodes must contain at least 3 entries. Since 2^40 is greater than 2^64, an r-tree structure always has a depth of 40 or less.
#define RTREE_MAX_DEPTH 40

//	An rtree cursor object.
type RtreeCursor struct {
	base		sqlite3_vtab_cursor
	pNode		*RtreeNode				//	Node cursor is currently pointing at
	iCell		int						//	Index of current cell in pNode
	iStrategy	int						//	Copy of idxNum search parameter
	nConstraint	int						//	Number of entries in aConstraint
	aConstraint	*RtreeConstraint		//	Search constraints.
}

union RtreeCoord {
  RtreeValue f;
  int i;
};

//	The argument is an RtreeCoord. Return the value stored within the RtreeCoord formatted as a float64 (float64 or int64). This macro assumes that local
//	variable pRtree points to the Rtree structure associated with the RtreeCoord.
//
//	can now abolish eCoordType?
func DCOORD(coord interface{}) (r float64) {
	switch c := coord.(type) {
	case int:
		r = float64(c)
	case float64:
		r = c
	}
	return
}


//	A search constraint.
type RtreeConstraint struct {
  iCoord	int							//	Index of constrained coordinate
  op		int							//	Constraining operation
  rValue	float64						//	Constraint value.
  xGeom		func(*sqlite3_rtree_geometry, []float64) (bool, int)
  pGeom		*sqlite3_rtree_geometry		//	Constraint callback argument for a MATCH
};

/* Possible values for RtreeConstraint.op */
#define RTREE_EQ    0x41
#define RTREE_LE    0x42
#define RTREE_LT    0x43
#define RTREE_GE    0x44
#define RTREE_GT    0x45
#define RTREE_MATCH 0x46

//	An rtree structure node.
type RtreeNode struct {
	pParent		*RtreeNode
	iNode		int64
	nRef		int
	isDirty		bool
	zData		[]byte
	pNext		*RtreeNode				//	Next node in this hash chain
}
#define NCELL(pNode) readInt16(&(pNode).zData[2])

//	Structure to store a deserialized rtree record.
type RtreeCell struct {
	iRowid		int64
	aCoord		[]float64
}

func NewRtreeCell(rowid int64, coords ...[]float64) (r *RtreeCell) {
	r = &RtreeCell{ iRowid: rowid, aCoord: make([]float64, RTREE_MAX_DIMENSIONS * 2)}
	if len(coords) > 0 {
		copy(r.aCoord, coords)
	}
	return
}

func (cell *RtreeCell) Duplicate() (r *RtreeCell) {
	if cell != nil {
		r = &RtreeCell{ iRowid: cell.iRowid, aCoord: make([]float64, len(cell.aCoord)) }
		copy(r.aCoord, cell.coords)
	}
	return
}

func (tree *Rtree) NewCell(rowid int64, coords []float64) (r *RtreeCell) {
	r = &RtreeCell{ iRowid: rowid, aCoord: make([]float64, tree.Dimensions * 2) }
	if len(coords) > 0 {
		copy(r.aCoord, coords)
	}
	return
}

//	Value for the first field of every RtreeMatchArg object. The MATCH operator tests that the first field of a blob operand matches this
//	value to avoid operating on invalid blobs (which could cause a segfault).
#define RTREE_GEOMETRY_MAGIC 0x891245AB

//	An instance of this structure must be supplied as a blob argument to the right-hand-side of an SQL MATCH operator used to constrain an r-tree query.
type RtreeMatchArg struct {
	magic		uint32				//	Always RTREE_GEOMETRY_MAGIC
	xGeom		func(*sqlite3_rtree_geometry, []float64) (bool, int)
	pContext	interface{}			//	void *
	nParam		int
	aParam[1]	float64
}

//	When a geometry callback is created (see sqlite3_rtree_geometry_callback), a single instance of the following structure is allocated. It is used
//	as the context for the user-function created by by s_r_g_c(). The object is eventually deleted by the destructor mechanism provided by
//	sqlite3_create_function_v2() (which is called by s_r_g_c() to create the geometry callback function).
type RtreeGeomCallback struct {
	xGeom		func(sqlite3_rtree_geometry*, []float64) (bool, int)
	pContext	interface{}			//	void *
}

#ifndef MAX
# define MAX(x,y) ((x) < (y) ? (y) : (x))
#endif
#ifndef MIN
# define MIN(x,y) ((x) > (y) ? (y) : (x))
#endif

//	Functions to deserialize a 16 bit integer, 32 bit real number and 64 bit integer. The deserialized value is returned.
static int readInt16(u8 *p) {
  return (p[0]<<8) + p[1];
}

func readCoord(p []byte) (coord RtreeCoord) {
	return u32(p[0]) << 24 + u32(p[1]) << 16 + u32(p[2]) <<  8 + u32(p[3]) <<  0
}

static i64 readInt64(u8 *p) {
  return (
    (((i64)p[0]) << 56) + 
    (((i64)p[1]) << 48) + 
    (((i64)p[2]) << 40) + 
    (((i64)p[3]) << 32) + 
    (((i64)p[4]) << 24) + 
    (((i64)p[5]) << 16) + 
    (((i64)p[6]) <<  8) + 
    (((i64)p[7]) <<  0)
  )
}

//	Functions to serialize a 16 bit integer, 32 bit real number and 64 bit integer. The value returned is the number of bytes written
//	to the argument buffer (always 2, 4 and 8 respectively).
func writeInt16(u8 *p, int i) int {
	p[0] = (i >> 8) & 0xFF
	p[1] = (i >> 0) & 0xFF
	return 2
}

func writeCoord(u8 *p, RtreeCoord *pCoord) int {
	assert( sizeof(RtreeCoord) == 4 )
	assert( sizeof(u32) == 4 )
	i := *(u32 *)pCoord
	p[0] = (i >> 24) & 0xFF
	p[1] = (i >> 16) & 0xFF
	p[2] = (i >> 8) & 0xFF
	p[3] = (i >> 0) & 0xFF
	return 4
}

static int writeInt64(u8 *p, i64 i) {
  p[0] = (i >> 56) & 0xFF
  p[1] = (i >> 48) & 0xFF
  p[2] = (i >> 40) & 0xFF
  p[3] = (i >> 32) & 0xFF
  p[4] = (i >> 24) & 0xFF
  p[5] = (i >> 16) & 0xFF
  p[6] = (i >> 8) & 0xFF
  p[7] = (i >> 0) & 0xFF
  return 8
}

//	Increment the reference count of node p.
func (p *RtreeNode) Reference() {
	if p != nil {
		p.nRef++
	}
}

//	Clear the content of node p (set all bytes to 0x00).
func (tree *Rtree) nodeZero(p *RtreeNode) {
	for i := tree.iNodeSize; i > 1; i-- {
		p.zData[i] = 0
	}
	p.isDirty = true
}

//	Given a node number iNode, return the corresponding key to use in the Rtree.aHash table.
func nodeHash(iNode int64) (r int) {
	r = (iNode >> 56) ^ (iNode >> 48) ^ (iNode >> 40) ^ (iNode >> 32) ^ (iNode >> 24) ^ (iNode >> 16) ^ (iNode >> 8) ^ (iNode >> 0)
	r %= HASHSIZE
	return
}

//	Search the node hash table for node iNode. If found, return a pointer to it. Otherwise, return 0.
func (tree *Rtree) nodeHashLookup(iNode int64) (p *RtreeNode) {
	for p = tree.aHash[nodeHash(iNode)]; p != nil && p.iNode != iNode; p = p.pNext {}
	return
}

//	Add node pNode to the node hash table.
func (tree *Rtree) nodeHashInsert(node *RtreeNode) {
	assert( pNode.pNext == 0 )
	iHash := nodeHash(node.iNode)
	node.pNext = tree.aHash[iHash]
	tree.aHash[iHash] = node
}

//	Remove node pNode from the node hash table.
func (tree *Rtree) nodeHashDelete(node *RtreeNode) {
	if node.iNode != 0 {
		p := &tree.aHash[nodeHash(node.iNode)]
		for ; (*p) != node; p = &(*p).pNext) {
			assert(*p)
		}
		*p = node.pNext
		node.pNext = 0
	}
}

//	Allocate and return new r-tree node. Initially, (RtreeNode.iNode == 0), indicating that node has not yet been assigned a node number. It is
//	assigned a node number when nodeWrite() is called to write the node contents out to the database.
func (tree *Rtree) nodeNew(parent *RtreeNode) (node *RtreeNode) {
	node = &RtreeNode{
		zData:		make([]byte, tree.iNodeSize),
		nRef:		1,
		pParent:	parent,
		isDirty:	true,
	}
	parent.Reference()
	return
}

//	Obtain a reference to an r-tree node.
func (tree *Rtree) nodeAcquire(iNode int64, parent *RtreeNode) (node *RtreeNode, rc int) {
	//	Check if the requested node is already in the hash table. If so, increase its reference count and return it.
	if node = tree.nodeHashLookup(iNode); node != nil {
		assert( parent == nil || node.pParent == nil || node.pParent == pParent )
		if parent && node.pParent == nil {
			parent.Reference()
			node.pParent = parent
		}
		node.nRef++
		return node, SQLITE_OK
	}

	sqlite3_bind_int64(tree.pReadNode, 1, iNode)
	if rc = tree.pReadNode.Step(); rc == SQLITE_ROW {
		zBlob := tree.pReadNode.ColumnBlob(0)
		if tree.iNodeSize == tree.pReadNode.ColumnBytes(0) {
			node = &RtreeNode{
				pParent: parent,
				zData: ([]byte)(&node[1]),
				nRef: 1,
				iNode: iNode,
				isDirty: false,
				pNext: nil,
			}
			copy(node.zData, zBlob)
			node.pParent.Reference()
		}
	}
	rc = tree.pReadNode.Reset()

	//	If the root node was just loaded, set pRtree.iDepth to the height of the r-tree structure. A height of zero means all data is stored on
	//	the root node. A height of one means the children of the root node are the leaves, and so on. If the depth as specified on the root node
	//	is greater than RTREE_MAX_DEPTH, the r-tree structure must be corrupt.
	if node != nil && iNode == 1 {
		tree.iDepth = readInt16(node.zData)
		if tree.iDepth > RTREE_MAX_DEPTH {
			rc = SQLITE_CORRUPT_VTAB
		}
	}

	//	If no error has occurred so far, check if the "number of entries" field on the node is too large. If so, set the return code to SQLITE_CORRUPT_VTAB.
	if node != nil && rc == SQLITE_OK {
		if NCELL(node) > ((tree.iNodeSize - 4) / tree.nBytesPerCell) {
			rc = SQLITE_CORRUPT_VTAB
		}
	}

	if rc == SQLITE_OK {
		if node != nil {
			tree.nodeHashInsert(node)
		} else {
			rc = SQLITE_CORRUPT_VTAB
		}
	} else {
		node = nil
	}
	return
}

//	Overwrite cell iCell of node pNode with the contents of pCell.
func (tree *Rtree) nodeOverwriteCell(node *RtreeNode, cell *RtreeCell, i int) {
	p := node.zData[4 + tree.nBytesPerCell * i]
	p += writeInt64(p, cell.iRowid)
	for j := 0; j < (tree.Dimensions * 2); j++ {
		p += writeCoord(p, &cell.aCoord[j])
	}
	node.isDirty = true
}

//	Remove cell the cell with index iCell from node pNode.
func (tree *Rtree) nodeDeleteCell(node *RtreeNode, i int) {
	pDst := &node.zData[4 + tree.nBytesPerCell * i]
	pSrc := &pDst[tree.nBytesPerCell]
	nByte := (NCELL(node) - i - 1) * tree.nBytesPerCell
	memmove(pDst, pSrc, nByte)
	writeInt16(&node.zData[2], NCELL(node) - 1)
	node.isDirty = true
}

//	Insert the contents of cell pCell into node pNode. If the insert is successful, return SQLITE_OK.
//	If there is not enough free space in pNode, return SQLITE_FULL.
func (tree *Rtree) nodeInsertCell(node *RtreeNode, cell *RtreeCell) int {
	int nCell;                    /* Current number of cells in pNode */
	int nMaxCell;                 /* Maximum number of cells for pNode */

	max_cell := (tree.iNodeSize - 4) / tree.nBytesPerCell
	n := NCELL(node)

	assert( n <= max_cell )
	if n < max_cell {
		tree.nodeOverwriteCell(node, cell, n)
		writeInt16(&pNode.zData[2], n + 1)
		node.isDirty = 1
	}
	return n == max_cell
}

//	If the node is dirty, write it out to the database.
func (tree *Rtree) nodeWrite(node *RtreeNode) (rc int) {
	if node.isDirty {
		p := tree.pWriteNode
		if node.iNode > 0 {
			sqlite3_bind_int64(p, 1, node.iNode)
		} else {
			sqlite3_bind_null(p, 1)
		}
		sqlite3_bind_blob(p, 2, node.zData, tree.iNodeSize, SQLITE_STATIC)
		p.Step()
		node.isDirty = false
		rc = p.Reset()
		if node.iNode == 0 && rc == SQLITE_OK {
			node.iNode = sqlite3_last_insert_rowid(tree.db)
			tree.nodeHashInsert(node)
		}
	}
	return
}

//	Release a reference to a node. If the node is dirty and the reference count drops to zero, the node data is written to the database.
func (tree *Rtree) nodeRelease(node *RtreeNode) (rc int) {
	if node != nil {
		assert( node.nRef > 0 )
		node.nRef--
		if node.nRef == 0 {
			if node.iNode == 1 {
				tree.iDepth = -1
			}
			if node.pParent != nil {
				rc = tree.nodeRelease(node.pParent)
			}
			if rc == SQLITE_OK {
				rc = tree.nodeWrite(node)
			}
			tree.nodeHashDelete(node)
			sqlite3_free(node)
		}
	}
	return
}

//	Return the 64-bit integer value associated with cell iCell of node pNode. If pNode is a leaf node, this is a rowid. If it is
//	an internal node, then the 64-bit integer is a child page number.
func (tree *Rtree) nodeGetRowid(node *RtreeNode, i int) int64 {
	assert( i < NCELL(node) )
	return readInt64(&node.zData[4 + tree.nBytesPerCell * i])
}

//	Return coordinate iCoord from cell iCell in node pNode.
func (tree *Rtree) nodeGetCoord(node *RtreeNode, iCell, iCoord int) (coord *RtreeCoord) {
	return readCoord(&node.zData[12 + tree.nBytesPerCell * iCell + 4 * iCoord])
}

//	Deserialize cell iCell of node pNode. Populate the structure pointed to by pCell with the results.
func (tree *Rtree) nodeGetCell(node *RtreeNode, iCell int) (cell *RtreeCell) {
	cell = tree.NewCell(tree.nodeGetRowid(node, iCell))
	for i := len(cell.aCoord) - 1; i > -1; i-- {
		cell.aCoord[i] = tree.nodeGetCoord(node, iCell, i)
	}
	return
}

//	Rtree virtual table module xCreate method.
//		pAux ===> void *
func rtreeCreate(db *sqlite3, pAux interface{}, args []string) (table *sqlite3_vtab, Err string, rc int) {
	return rtreeInit(db, pAux, args, true)
}

//	Rtree virtual table module xConnect method.
//		pAux ===> void *
func rtreeConnect(db *sqlite3, pAux interface{}, args []string) (table *sqlite3_vtab, Err string, rc int) {
	return rtreeInit(db, pAux, args, false)
}

//	Increment the r-tree reference count.
func (tree *Rtree) Reference() {
	tree.nBusy++
}

//	Decrement the r-tree reference count. When the reference count reaches zero the structure is deleted.
func (tree *Rtree) Release() {
	if tree.nBusy--; pRtree.nBusy == 0 {
		tree.pReadNode.Finalize()
		tree.pWriteNode.Finalize()
		tree.pDeleteNode.Finalize()
		tree.pReadRowid.Finalize()
		tree.pWriteRowid.Finalize()
		tree.pDeleteRowid.Finalize()
		tree.pReadParent.Finalize()
		tree.pWriteParent.Finalize()
		tree.pDeleteParent.Finalize()
		sqlite3_free(tree)
	}
}

//	Rtree virtual table module xDisconnect method.
func rtreeDisconnect(table *sqlite3_vtab) int{
	tree.Release((Rtree *)(table))
	return SQLITE_OK
}

//	Rtree virtual table module xDestroy method.
func rtreeDestroy(table *sqlite3_vtab) (rc int) {
	tree := (Rtree *)(table)
	zCreate := sqlite3_mprintf(
		"DROP TABLE '%q'.'%q_node'; DROP TABLE '%q'.'%q_rowid'; DROP TABLE '%q'.'%q_parent';",
		tree.zDb, tree.zName, 
		tree.zDb, tree.zName,
		tree.zDb, tree.zName
	)
	rc = sqlite3_exec(tree.db, zCreate, 0, 0, 0)
	sqlite3_free(zCreate)
	if rc == SQLITE_OK {
		tree.Release()
	}
	return
}

//	Rtree virtual table module xOpen method.
//		cursor ===> *sqlite3_vtab_cursor
func rtreeOpen(table *sqlite3_vtab) (cursor *RtreeCursor) {
	cursor = new(RtreeCursor)
	cursor.base.pVtab = table
	return
}

//	Free the RtreeCursor.aConstraint[] array and its contents.
func (cursor *RtreeCursor) freeConstraints() {
	if cursor.aConstraint != nil {
		for i := 0; i < cursor.nConstraint; i++ {
			pGeom := cursor.aConstraint[i].pGeom
			if pGeom != nil {
				if pGeom.xDelUser != nil {
					pGeom.xDelUser(pGeom.pUser)
				}
				sqlite3_free(pGeom)
			}
		}
		sqlite3_free(cursor.aConstraint)
		cursor.aConstraint = nil
	}
}

//	Rtree virtual table module xClose method.
func rtreeClose(cursor *sqlite3_vtab_cursor) (rc int) {
	tree := (Rtree *)(cursor.pVtab)
	pCsr := (RtreeCursor *)(cursor)
	pCsr.freeCursorConstraints()
	rc = tree.nodeRelease(pCsr.pNode)
	sqlite3_free(pCsr)
	return
}

//	Rtree virtual table module xEof method.
//	Return non-zero if the cursor does not currently point to a valid record (i.e if the scan has finished), or zero otherwise.
func rtreeEof(cursor *sqlite3_vtab_cursor) int {
	pCsr := (RtreeCursor *)(cursor)
	return pCsr.pNode == 0
}

//	The r-tree constraint passed as the second argument to this function is guaranteed to be a MATCH constraint.
//		pbRes	OUT: Test result
func (tree *Rtree) testGeom(pConstraint *RtreeConstraint, cell *RtreeCell) (isEof bool, rc int) {
	assert( pConstraint.op == RTREE_MATCH )
	assert( pConstraint.pGeom != nil )

	aCoord := make([]float64, tree.Dimensions * 2)
	for i := len(aCoord) - 1; i > -1; i-- {
		aCoord[i] = DCOORD(cell.aCoord[i])
	}
	return pConstraint.xGeom(pConstraint.pGeom, aCoord)
}

//	Cursor pCursor currently points to a cell in a non-leaf page. Set *pbEof to true if the sub-tree headed by the cell is filtered
//	(excluded) by the constraints in the pCursor->aConstraint[] array, or false otherwise.
//	Return SQLITE_OK if successful or an SQLite error code if an error occurs within a geometry callback.
func (tree *Rtree) testCell(cursor *RtreeCursor) (isEof bool, rc int) {
	cell := tree.nodeGetCell(cursor.pNode, cursor.iCell)
	for i := 0; !isEof && i < cursor.nConstraint; i++ {
		p := &cursor.aConstraint[i]
		cell_min := DCOORD(cell.aCoord[(p.iCoord >> 1) * 2])
		cell_max := DCOORD(cell.aCoord[(p.iCoord >> 1) * 2 + 1])

		assert( p.op == RTREE_LE || p.op == RTREE_LT || p.op == RTREE_GE || p.op == RTREE_GT || p.op == RTREE_EQ || p.op == RTREE_MATCH )

		switch p.op {
		case RTREE_LE: fallthrough
		case RTREE_LT: 
			isEof = p.rValue < cell_min

		case RTREE_GE: fallthrough
		case RTREE_GT: 
			isEof = p.rValue > cell_max

		case RTREE_EQ:
			isEof = p.rValue > cell_max || p.rValue < cell_min

		default:
			assert( p.op == RTREE_MATCH )
			rc = tree.testGeom(p, &cell, &isEof)
			isEof = !isEof
		}
	}
	return
}

//	Test if the cell that cursor pCursor currently points to would be filtered (excluded) by the constraints in the 
//	cursor.aConstraint[] array. If so, set *pbEof to true before returning. If the cell is not filtered (excluded) by the constraints,
//	set pbEof to zero.
//	Return SQLITE_OK if successful or an SQLite error code if an error occurs within a geometry callback.
//	This function assumes that the cell is part of a leaf node.
func (tree *Rtree) testEntry(cursor *RtreeCursor) (isEof bool, rc int) {
	cell := tree.nodeGetCell(cursor.pNode, cursor.iCell)
	for i := 0; i < cursor.nConstraint; i++ {
		p := &cursor.aConstraint[i]
		coord := DCOORD(cell.aCoord[p.iCoord])

		assert(p.op == RTREE_LE || p.op == RTREE_LT || p.op == RTREE_GE || p.op == RTREE_GT || p.op == RTREE_EQ || p.op == RTREE_MATCH)
		switch res := false; p.op {
		case RTREE_LE:
			res = coord <= p.rValue
		case RTREE_LT:
			res = coord < p.rValue
		case RTREE_GE:
			res = coord >= p.rValue
		case RTREE_GT:
			res = coord > p.rValue
		case RTREE_EQ:
			res = coord == p.rValue)
		default:
			assert( p.op == RTREE_MATCH )
			if res, rc = tree.testGeom(p, cell); rc != SQLITE_OK {
				return
			}
		}

		if res == false {
			return true, SQLITE_OK
		}
	}
	return false, SQLITE_OK
}

//	Cursor pCursor currently points at a node that heads a sub-tree of height iHeight (if iHeight==0, then the node is a leaf). Descend
//	to point to the left-most cell of the sub-tree that matches the configured constraints.
//			pEof ==> OUT: Set to true if cannot descend
func (tree *Rtree) descendToCell(cursor *RtreeCursor, height int) (isEof bool, rc int) {
	saved_node := cursor.pNode
	saved_cell := cursor.iCell

	assert( height >= 0 )
	if height == 0 {
		isEof, rc = tree.testEntry(cursor)
	} else {
		isEof, rc = tree.testCell(cursor)
	}
	if rc != SQLITE_OK || isEof || height == 0 {
		return
	}

	var child	*RtreeNode
	if child, rc = tree.nodeAcquire(tree.nodeGetRowid(cursor.pNode, cursor.iCell), cursor.pNode); rc != SQLITE_OK {
		return
	}

	tree.nodeRelease(cursor.pNode)
	cursor.pNode = child
	isEof = true
	for i := 0; isEof && i < NCELL(child); i++ {
		cursor.iCell = i
		if isEof, rc = tree.descendToCell(cursor, height - 1); rc != SQLITE_OK {
			return
		}
	}

	if isEof {
		assert( cursor.pNode == child )
		saved_node.Reference()
		tree.nodeRelease(child)
		cursor.pNode = saved_node
		cursor.iCell = saved_cell
	}
	return
}

//	One of the cells in node pNode is guaranteed to have a 64-bit integer value equal to iRowid. Return the index of this cell.
func (tree *Rtree) nodeRowidIndex(node *RtreeNode, iRowid int64) (i, rc int) {
	for i = 0; i < NCELL(node); i++ {
		if tree.nodeGetRowid(node, i) == iRowid {
			return
		}
	}
	return 0, SQLITE_CORRUPT_VTAB
}

//	Return the index of the cell containing a pointer to node pNode in its parent. If pNode is the root node, return -1.
func (tree *Rtree) nodeParentIndex(pNode *RtreeNode) (index, rc int){
	if parent := pNode.pParent; pParent != nil {
		return tree.nodeRowidIndex(pParent, pNode.iNode)
	}
	return -1, SQLITE_OK
}

//	Rtree virtual table module xNext method.
func rtreeNext(pVtabCursor *sqlite3_vtab_cursor) (rc int) {
	tree := (Rtree *)(pVtabCursor.pVtab)
	cursor := (RtreeCursor *)(pVtabCursor)

	//	RtreeCursor.pNode must not be NULL. If is is NULL, then this cursor is already at EOF. It is against the rules to call the xNext() method of
	//	a cursor that has already reached EOF.

	assert( cursor.pNode )
	if cursor.iStrategy == 1 {
		//	This "scan" is a direct lookup by rowid. There is no next entry.
		tree.nodeRelease(cursor.pNode)
		cursor.pNode = nil
	} else {
		//	Move to the next entry that matches the configured constraints.
		height := 0
		for ; cursor.pNode != nil; {
			node := cursor.pNode
			nCell := NCELL(node)
			for cursor.iCell++; cursor.iCell < nCell; cursor.iCell++ {
				if isEof, rc := tree.descendToCell(cursor, height); rc != SQLITE_OK || !isEof {
					return rc
				}
			}
			cursor.pNode = node.pParent
			if rc = tree.nodeParentIndex(node, &cursor.iCell); rc != SQLITE_OK {
				return
			}
			cursor.pNode.Reference()
			tree.nodeRelease(node)
			height++
		}
	}
	return
}

//	Rtree virtual table module xRowid method.
func rtreeRowid(pVtabCursor *sqlite3_vtab_cursor) (rowid int64, rc int) {
	tree := (Rtree *)(pVtabCursor.pVtab)
	cursor := (RtreeCursor *)(pVtabCursor)

	assert(cursor.pNode)
	return tree.nodeGetRowid(cursor.pNode, cursor.iCell), SQLITE_OK
}

//	Rtree virtual table module xColumn method.
func rtreeColumn(pVtabCursor *sqlite3_vtab_cursor, ctx *sqlite3_context, i int) int {
	tree := (Rtree *)(pVtabCursor.pVtab)
	cursor := (RtreeCursor *)(pVtabCursor)

	if i == 0 {
		iRowid := tree.nodeGetRowid(cursor.pNode, cursor.iCell)
		sqlite3_result_int64(ctx, iRowid)
	} else {
		c := tree.nodeGetCoord(cursor.pNode, cursor.iCell, i - 1)
		if pRtree.eCoordType == RTREE_COORD_REAL32 {
			sqlite3_result_float64(ctx, c.f)
		} else {
			assert( tree.eCoordType == RTREE_COORD_INT32 )
			sqlite3_result_int(ctx, c.i)
		}
	}
	return SQLITE_OK
}

//	Use nodeAcquire() to obtain the leaf node containing the record with rowid iRowid. If successful, set *ppLeaf to point to the node and
//	return SQLITE_OK. If there is no such record in the table, set *ppLeaf to 0 and return SQLITE_OK. If an error occurs, set *ppLeaf
//	to zero and return an SQLite error code.
func (tree *Rtree) findLeafNode(rowid int64) (pLeaf *RtreeNode, rc int) {
	sqlite3_bind_int64(tree.pReadRowid, 1, rowid)
	if tree.pReadRowid.Step() == SQLITE_ROW {
		iNode := sqlite3_column_int64(tree.pReadRowid, 0)
		pLeaf, rc = tree.nodeAcquire(iNode, nil)
		tree.pReadRowid.Reset()
	} else {
		rc = tree.pReadRowid.Reset()
	}
	return
}

//	This function is called to configure the RtreeConstraint object passed as the second argument for a MATCH constraint. The value passed as the
//	first argument to this function is the right-hand operand to the MATCH operator.

func deserializeGeometry(pValue *sqlite3_value, pCons *RtreeConstraint) (rc int) {
	var p		*RtreeMatchArg
	var pGeom	*sqlite3_rtree_geometry
	var nBlob	int

	//	Check that value is actually a blob.
	if sqlite3_value_type(pValue) != SQLITE_BLOB {
		return SQLITE_ERROR
	}

	//	Check that the blob is roughly the right size.
	if nBlob = sqlite3_value_bytes(pValue); nBlob < (int)sizeof(RtreeMatchArg) || ((nBlob - sizeof(RtreeMatchArg)) % sizeof(float64)) != 0 {
		return SQLITE_ERROR
	}

	if pGeom = (sqlite3_rtree_geometry *)sqlite3_malloc(sizeof(sqlite3_rtree_geometry) + nBlob); pGeom == nil {
		return SQLITE_NOMEM
	}
	memset(pGeom, 0, sizeof(sqlite3_rtree_geometry))
	p = (RtreeMatchArg *)&pGeom[1]

	memcpy(p, sqlite3_value_blob(pValue), nBlob)
	if p.magic != RTREE_GEOMETRY_MAGIC || nBlob != int(sizeof(RtreeMatchArg) + (p.nParam-  1) * sizeof(float64))) {
		sqlite3_free(pGeom)
		return SQLITE_ERROR
	}

	pGeom.pContext = p.pContext
	pGeom.nParam = p.nParam
	pGeom.aParam = p.aParam

	pCons.xGeom = p.xGeom
	pCons.pGeom = pGeom
	return SQLITE_OK
}

//	Rtree virtual table module xFilter method.
func rtreeFilter(pVtabCursor *sqlite3_vtab_cursor, idxNum int, idxStr string, argc int, argv []*sqlite3_value) (rc int) {
	tree := (Rtree *)(pVtabCursor.pVtab)
	pCsr := (RtreeCursor *)(pVtabCursor)
	tree.Reference()
	pCsr.freeConstraints()
	pCsr.iStrategy = idxNum

	if idxNum == 1 {
		//	Special case - lookup by rowid.
		var pLeaf	*RtreeNode			//	Leaf on which the required cell resides
		iRowid := sqlite3_value_int64(argv[0])
		pLeaf, rc = tree.findLeafNode(iRowid)
		pCsr.pNode = pLeaf
		if pLeaf != nil {
			assert( rc == SQLITE_OK )
			pCsr.iCell, rc = tree.nodeRowidIndex(pLeaf, iRowid)
		}
	} else {
		//	Normal case - r-tree scan. Set up the RtreeCursor.aConstraint array with the configured constraints. 
		if argc > 0 {
			pCsr.aConstraint = sqlite3_malloc(sizeof(RtreeConstraint) * argc)
			pCsr.nConstraint = argc
			if !pCsr.aConstraint {
				rc = SQLITE_NOMEM
			} else {
				memset(pCsr.aConstraint, 0, sizeof(RtreeConstraint) * argc)
				assert( (idxStr == 0 && argc == 0) || (idxStr && (int)strlen(idxStr) == argc * 2) )
				for i := 0; i < argc; i++ {
					p = &pCsr.aConstraint[i]
					p.op = idxStr[i * 2]
					p.iCoord = idxStr[i * 2 + 1] - 'a'
					if p.op == RTREE_MATCH {
						//	A MATCH operator. The right-hand-side must be a blob that can be cast into an RtreeMatchArg object. One created using
						//	an sqlite3_rtree_geometry_callback() SQL user function.
						if rc = deserializeGeometry(argv[i], p); rc != SQLITE_OK {
							break
						}
					} else {
						p.rValue = sqlite3_value_float64(argv[i])
					}
				}
			}
		}

		var root	*RtreeNode
		if rc == SQLITE_OK {
			pCsr.pNode = nil
			root, rc = tree.nodeAcquire(1, nil)
		}

		if rc == SQLITE_OK {
			nCell := NCELL(root)
			pCsr.pNode = root
			for pCsr.iCell = 0; rc == SQLITE_OK && pCsr.iCell < nCell; pCsr.iCell++ {
				assert( pCsr.pNode == root )
				if isEof, rc = tree.descendToCell(pCsr, tree.iDepth); !isEof {
					break
				}
			}
			if rc == SQLITE_OK && isEof {
				assert( pCsr.pNode == root )
				tree.nodeRelease(root)
				pCsr.pNode = nil
			}
			assert( rc != SQLITE_OK || pCsr.pNode == nil || pCsr.iCell < NCELL(pCsr.pNode) )
		}
	}
	pRtree.Release()
	return
}

//	Rtree virtual table module xBestIndex method. There are three table scan strategies to choose from (in order from most to least desirable):
//
//		idxNum     idxStr        Strategy
//		------------------------------------------------
//		  1        Unused        Direct lookup by rowid.
//		  2        See below     R-tree query or full-table scan.
//		------------------------------------------------
//
//	If strategy 1 is used, then idxStr is not meaningful. If strategy 2 is used, idxStr is formatted to contain 2 bytes for each 
//	constraint used. The first two bytes of idxStr correspond to the constraint in sqlite3_index_info.aConstraintUsage[] with (argvIndex==1) etc.
//
//	The first of each pair of bytes in idxStr identifies the constraint operator as follows:
//
//		Operator    Byte Value
//		----------------------
//		   =        0x41 ('A')
//		  <=        0x42 ('B')
//		   <        0x43 ('C')
//		  >=        0x44 ('D')
//		   >        0x45 ('E')
//		MATCH       0x46 ('F')
//		----------------------
//
//	The second of each pair of bytes identifies the coordinate column to which the constraint applies. The leftmost coordinate column
//	is 'a', the second from the left 'b' etc.

func rtreeBestIndex(tab *sqlite3_vtab, pIdxInfo *sqlite3_index_info) (rc int) {
	var iIdx	int

	char zIdxStr[RTREE_MAX_DIMENSIONS * 8 + 1]

	UNUSED_PARAMETER(tab);

	assert( pIdxInfo.idxStr == 0 )
	for i := 0; i < pIdxInfo.nConstraint && iIdx < int(sizeof(zIdxStr) - 1); i++ {
		p := pIdxInfo.aConstraint[i]

		if p.usable && p.iColumn == 0 && p.op == SQLITE_INDEX_CONSTRAINT_EQ {
			//	We have an equality constraint on the rowid. Use strategy 1.
			var j	int
			for ; j < i; j++ {
				pIdxInfo.aConstraintUsage[j].argvIndex = 0
				pIdxInfo.aConstraintUsage[j].omit = false
			}
			pIdxInfo.idxNum = 1
			pIdxInfo.aConstraintUsage[i].argvIndex = 1
			pIdxInfo.aConstraintUsage[j].omit = true

			//	This strategy involves a two rowid lookups on an B-Tree structures and then a linear search of an R-Tree node. This should be 
			//	considered almost as quick as a direct rowid lookup (for which sqlite uses an internal cost of 0.0).
			pIdxInfo.estimatedCost = 10.0
			return SQLITE_OK
		}

		if p.usable && (p.iColumn > 0 || p.op == SQLITE_INDEX_CONSTRAINT_MATCH) {
			u8 op
			switch p.op {
			case SQLITE_INDEX_CONSTRAINT_EQ:
				op = RTREE_EQ
			case SQLITE_INDEX_CONSTRAINT_GT:
				op = RTREE_GT
			case SQLITE_INDEX_CONSTRAINT_LE:
				op = RTREE_LE
			case SQLITE_INDEX_CONSTRAINT_LT:
				op = RTREE_LT
			case SQLITE_INDEX_CONSTRAINT_GE:
				op = RTREE_GE
			default:
				assert( p.op == SQLITE_INDEX_CONSTRAINT_MATCH )
				op = RTREE_MATCH
			}
			zIdxStr[iIdx++] = op
			zIdxStr[iIdx++] = p.iColumn - 1 + 'a'
			pIdxInfo.aConstraintUsage[i].argvIndex = iIdx / 2
			pIdxInfo.aConstraintUsage[i].omit = true
		}
	}

	pIdxInfo.idxNum = 2
	pIdxInfo.needToFreeIdxStr = true
	pIdxInfo.idxStr = sqlite3_mprintf("%s", zIdxStr)
	assert( iIdx >= 0 )
	pIdxInfo.estimatedCost = 2000000.0 / float64(iIdx + 1)
	return
}

//	Return the N-dimensional volumn of the cell stored in *p
func (tree *Rtree) Area(cell *RtreeCell) (area float64) {
	area = 1
	for i := 0; i < tree.Dimensions * 2; i += 2 {
		area *= DCOORD(cell.aCoord[i + 1]) - DCOORD(cell.aCoord[i])
	}
	return
}

//	Return the margin length of cell p. The margin length is the sum of the objects size in each dimension.
func (tree *Rtree) Margin(cell *RtreeCell) (margin float64) {
	for i := 0; i < tree.Dimensions * 2; i += 2 {
		margin += DCOORD(p.aCoord[i + 1]) - DCOORD(p.aCoord[i])
	}
	return
}

//	Store the union of cells p1 and p2 in p1.
func (tree *Rtree) Union(c1, c2 *RtreeCell) {
	if tree.eCoordType == RTREE_COORD_REAL32 {
		for i := 0; i < tree.Dimensions * 2; i += 2 {
			c1.aCoord[i].f = MIN(c1.aCoord[i].f, c2.aCoord[i].f)
			c1.aCoord[i + 1].f = MAX(c1.aCoord[i + 1].f, c2.aCoord[i + 1].f)
		}
	} else {
		for i := 0; i < pRtree.Dimensions * 2; i += 2 {
			c1.aCoord[i].i = MIN(c1.aCoord[i].i, c2.aCoord[i].i)
			c1.aCoord[i + 1].i = MAX(c1.aCoord[i + 1].i, c2.aCoord[i + 1].i)
		}
	}
}


//	Return true if the area covered by p2 is a subset of the area covered by p1. False otherwise.
func (tree *Rtree) Contains(c1, c2 *RtreeCell) bool {
	isInt := (pRtree.eCoordType == RTREE_COORD_INT32)
	for i := 0; i < tree.Dimensions * 2; i += 2 {
		a1 := &c1.aCoord[i]
		a2 := &c2.aCoord[i]
		if (!isInt && (a2[0].f < a1[0].f || a2[1].f > a1[1].f)) || ( isInt && (a2[0].i < a1[0].i || a2[1].i > a1[1].i)) {
			return false
		}
	}
	return true
}

//	Return the amount cell p would grow by if it were unioned with pCell.
func (tree *Rtree) UnionGrowth(p, pCell *RtreeCell) (area float64) {
	cell := tree.NewCell(p.iRowid, p.aCoord...)
	area = tree.Area(&cell)
	tree.Union(&cell, pCell)
	area = tree.Area(&cell) - area
	return
}

#if VARIANT_RSTARTREE_CHOOSESUBTREE || VARIANT_RSTARTREE_SPLIT
func (tree *Rtree) Overlap(p *RtreeCell, cells []*RtreeCell, iExclude int) (overlap float64) {
	for i, cell := range cells {
#if VARIANT_RSTARTREE_CHOOSESUBTREE
		if i != iExclude
#else
		assert( iExclude == -1 )
		UNUSED_PARAMETER(iExclude)
#endif
		{
			o := float64(1)
			for j := 0; j < tree.Dimensions * 2; j += 2 {
				x1 := MAX(DCOORD(p.aCoord[j]), DCOORD(cell.aCoord[j]))
				x2 := MIN(DCOORD(p.aCoord[j + 1]), DCOORD(cell.aCoord[j + 1]))
				if x2 < x1 {
					o = 0.0
					break
				} else {
					o = o * (x2 - x1)
				}
			}
			overlap += o
		}
	}
	return overlap
}
#endif

#if VARIANT_RSTARTREE_CHOOSESUBTREE
func (tree *Rtree) OverlapEnlargement(p, pInsert *RtreeCell, cells []*RtreeCell, iExclude int) float64 {
	before := tree.Overlap(p, cells, iExclude)
	tree.Union(p, pInsert)
	after := tree.Overlap(p, cells, iExclude)
	return after - before
}
#endif

//	This function implements the ChooseLeaf algorithm from Gutman[84]. ChooseSubTree in r*tree terminology.
func (tree *Rtree) ChooseLeaf(cell *RtreeCell, height int) (node *RtreeNode, rc int) {
	node, rc = tree.nodeAcquire(1, nil)
	for i := 0; rc == SQLITE_OK && i < tree.iDepth - height; i++ {
		var best_rowid			int64
		var MinGrowth, MinArea	float64
#if VARIANT_RSTARTREE_CHOOSESUBTREE
		var MinOverlap, overlap	float64
#endif

		nCell := NCELL(node)
		var cells	[]*RtreeCell
#if VARIANT_RSTARTREE_CHOOSESUBTREE
		if i == tree.iDepth - 1 {
			cells = make([]RtreeCell, nCell)
			for j := 0; j < nCell; j++ {
				cells[j] = tree.nodeGetCell(node, j)
			}
		}
#endif

		//	Select the child node which will be enlarged the least if cell is inserted into it. Resolve ties by choosing the entry with the smallest area.
		for iCell := 0; iCell < nCell; iCell++ {
			c := tree.nodeGetCell(node, iCell)
			growth := tree.cellGrowth(&c, cell)
			area := tree.Area(&c)

#if VARIANT_RSTARTREE_CHOOSESUBTREE
			if i == tree.iDepth - 1 {
				overlap = tree.OverlapEnlargement(&c, cell, cells, iCell)
			} else {
				overlap = 0.0
			}
			if iCell == 0 || overlap < MinOverlap || (overlap == MinOverlap && growth < MinGrowth) || (overlap == MinOverlap && growth == MinGrowth && area < MinArea) {
				MinGrowth = growth
				MinArea = area
				MinOverlap = overlap
				best_rowid = c.iRowid
			}
#else
			if iCell == 0 || growth < MinGrowth: || (growth == MinGrowth && area < MinArea) {
				MinGrowth = growth
				MinArea = area
				best_rowid = c.iRowid
			}
#endif
		}

		var child	*RtreeNode
		child, rc = tree.nodeAcquire(best_rowid, node)
		tree.nodeRelease(node)
		node = child
	}
	return
}

//	A cell with the same content as pCell has just been inserted into the node pNode. This function updates the bounding box cells in all ancestor elements.
func (tree *Rtree) AdjustTree(node *RtreeNode, cell *RtreeCell) int {
	for p := node; p.pParent != nil; {
		parent := p.pParent
		if iCell, rc := tree.nodeParentIndex(p); rc == SQLITE_OK {
			c := tree.nodeGetCell(parent, iCell)
			if !tree.Contains(c, cell) {
				tree.Union(c, cell)
				tree.nodeOverwriteCell(parent, c, iCell)
			}
			p = parent
		} else {
			return SQLITE_CORRUPT_VTAB
		}
	}
	return
}

//	Write mapping (rowid->node) to the <rtree>_rowid table.
func (tree *Rtree) rowidWrite(rowid, node int64) int {
	sqlite3_bind_int64(tree.pWriteRowid, 1, rowid)
	sqlite3_bind_int64(tree.pWriteRowid, 2, node)
	tree.pWriteRowid.Step()
	return tree.pWriteRowid.Reset()
}

//	Write mapping (node->parent) to the <rtree>_parent table.
func (tree *Rtree) parentWrite(node, parent int64) int {
	sqlite3_bind_int64(tree.pWriteParent, 1, node)
	sqlite3_bind_int64(tree.pWriteParent, 2, parent)
	tree.pWriteParent.Step()
	return tree.pWriteParent.Reset()
}

#if VARIANT_GUTTMAN_LINEAR_SPLIT
//	Implementation of the linear variant of the PickNext() function from Guttman[84].
func (tree *Rtree) LinearPickNext(cells []*RtreeCell, LeftBox, RightBox *RtreeCell, Used []bool) *RtreeCell {
	var i	int
	for i = 0; aiUsed[i]; i++ {}
	aiUsed[i] = true
	return cells[i]
}

//	Implementation of the linear variant of the PickSeeds() function from Guttman[84].
func (tree *Rtree) LinearPickSeeds(cells []*RtreeCell) (LeftSeed, RightSeed int) {
	var maxNormalInnerWidth	float64
	RightSeed = 1

	//	Pick two "seed" cells from the array of cells. The algorithm used here is the LinearPickSeeds algorithm from Gutman[1984]. The 
	//	indices of the two seed cells in the array are stored in local variables iLeftSeek and iRightSeed.
	for i := 0; i < tree.Dimensions; i++ {
		x1 := DCOORD(cells[0].aCoord[i * 2])
		x2 := DCOORD(cells[0].aCoord[i * 2 + 1])
		x3 := x1
		x4 := x2

		var LeftCell, RightCell	int

		for j, cell := range cells {
			switch left := DCOORD(cell.aCoord[i * 2]); {
			case left < x1:
				x1 = left
			case left > x3:
				x3 = left
				RightCell = j
			}

			switch right := DCOORD(cell.aCoord[i * 2 + 1]); {
			case right > x4:
				x4 = right
			case right < x2:
				x2 = right
				LeftCell = j
			}
		}

		if x4 != x1 {
			normalwidth := (x3 - x2) / (x4 - x1)
			if normalwidth > maxNormalInnerWidth {
				LeftSeed = LeftCell
				RightSeed = RightCell
			}
		}
	}
	return
}
#endif /* VARIANT_GUTTMAN_LINEAR_SPLIT */

#if VARIANT_GUTTMAN_QUADRATIC_SPLIT
//	Implementation of the quadratic variant of the PickNext() function from Guttman[84].
func (tree *Rtree) QuadraticPickNext(cells []*RtreeCell, LeftBox, RightBox *RtreeCell, Used []bool) *RtreeCell {
	var fDiff	float64
	selected := -1
	for i := 0; i < nCell; i++ {
		if !Used[i] {
			left := tree.cellGrowth(pLeftBox, cells[i])
			right := tree.cellGrowth(pLeftBox, cells[i])
			diff := math.Abs(right - left)
			if selected < 0 || diff > fDiff {
				fDiff = diff
				selected = i
			}
		}
	}
	Used[selected] = true
	return cells[selected]
}

//	Implementation of the quadratic variant of the PickSeeds() function from Guttman[84].
func (tree *Rtree) QuadraticPickSeeds(cells []*RtreeCell) (LeftSeed, RightSeed int) {
	var fWaste	float64
	RightSeed = 1
	for i, start_cell := range cells {
		for j, next_cell := range cells[start_cell + 1:] {
			right := tree.Area(next_cell)
			growth := tree.cellGrowth(start_cell, next_cell)
			waste := growth - right

			if waste > fWastUnionGrowth {
				LeftSeed = i
				RightSeed = j
				fWaste = waste
			}
		}
	}
}
#endif /* VARIANT_GUTTMAN_QUADRATIC_SPLIT */

//	Arguments aIdx, aDistance and aSpare all point to arrays of size nIdx. The aIdx array contains the set of integers from 0 to 
//	(nIdx-1) in no particular order. This function sorts the values in aIdx according to the indexed values in aDistance. For
//	example, assuming the inputs:
//
//		aIdx      = { 0,   1,   2,   3 }
//		aDistance = { 5.0, 2.0, 7.0, 6.0 }
//
//	this function sets the aIdx array to contain:
//
//		aIdx      = { 0,   1,   2,   3 }
//
//	The aSpare array is used as temporary working space by the sorting algorithm.
func SortByDistance(Idx []int, Distance []float64, Spare []int) {
	if nIdx := len(Idx); nIdx >1 {
		var Left, Right		int

		nLeft := nIdx / 2
		nRight := nIdx - nLeft
		aLeft := Idx[:nLeft]
		aRight = Idx[nLeft:]

		SortByDistance(aLeft, Distance, Spare)
		SortByDistance(aRight, Distance, Spare)

		copy(Spare, aLeft)
		aLeft = Spare

		for ; Left < nLeft || Right < nRight {
			if Left == nLeft {
				Idx[Left + Right] = aRight[Right]
				Right++
			} else if Right == nRight {
				Idx[Left + Right] = aLeft[Left]
				Left++
			} else {
				fLeft := aDistance[aLeft[Left]]
				fRight := aDistance[aRight[Right]]
				if fLeft < fRight {
					Idx[Left + Right] = aLeft[Left]
					Left++
				} else {
					Idx[Left + Right] = aRight[Right]
					Right++
				}
			}
		}
	}
}

//	Arguments aIdx, aCell and aSpare all point to arrays of size nIdx. The aIdx array contains the set of integers from 0 to 
//	(nIdx-1) in no particular order. This function sorts the values in aIdx according to dimension iDim of the cells in aCell. The
//	minimum value of dimension iDim is considered first, the maximum used to break ties.
//
//	The aSpare array is used as temporary working space by the sorting algorithm.
func (tree *Rtree) SortByDimension(Idx []int, Dim int, cells []*RtreeCell, Spare []int) {
	if nIdx := len(Idx); nIdx > 1 {
		var Left, Right		int
		nLeft := nIdx / 2
		nRight := nIdx - nLeft
		aLeft := Idx[:nLeft]
		aRight := Idx[nLeft:]

		tree.SortByDimension(aLeft, Dim, cells, Spare)
		tree.SortByDimension(aRight, Dim, cells, Spare)
		copy(Spare, aLeft)
		aLeft = Spare

		for ; Left < nLeft || Right < nRight; {
			xleft1 := DCOORD(cells[aLeft[Left]].aCoord[Dim * 2])
			xleft2 := DCOORD(cells[aLeft[Left]].aCoord[Dim * 2 + 1])
			xright1 := DCOORD(cells[aRight[Right]].aCoord[Dim * 2])
			xright2 := DCOORD(cells[aRight[Right]].aCoord[Dim * 2 + 1])
			if (Left != nLeft && Right == nRight) || (xleft1 < xright1) || (xleft1 == xright1 && xleft2 < xright2) {
				Idx[Left + Right] = aLeft[Left]
				Left++
			} else {
				Idx[Left + Right] = aRight[Right]
				Right++
			}
		}
	}
}

#if VARIANT_RSTARTREE_SPLIT
//	Implementation of the R*-tree variant of SplitNode from Beckman[1990].
func (tree *Rtree) splitNodeStartree(cells []*RtreeCell, Left, Right *RtreeNode) (BboxLeft, BboxRight *RtreeCell, rc int) {
	int **aaSorted;
	int *aSpare;

	var BestDim, BestSplit	int
	var BestMargin			float64

	nByte := (tree.Dimensions + 1) * (sizeof(int*) + nCell * sizeof(int))
	aaSorted = (int **)sqlite3_malloc(nByte)
	if !aaSorted {
		return SQLITE_NOMEM;
	}

	aSpare = &((int *)&aaSorted[tree.Dimensions])[tree.Dimensions * nCell]
	memset(aaSorted, 0, nByte)

	for i := 0; i < tree.Dimensions; i++ {
		aaSorted[i] = &((int *)&aaSorted[tree.Dimensions])[i * nCell]
		for j := 0; j < nCell; j++ {
			aaSorted[i][j] = j
		}
		tree.SortByDimension(aaSorted[i], nCell, i, aCell, aSpare)
	}

	for i := 0; i < tree.Dimensions; i++ {
		var margin, BestOverlap, BestArea	float64
		var BestLeft, nLeft					int

		for nLeft = RTREE_MINCELLS(tree); nLeft <= (nCell - RTREE_MINCELLS(tree)); nLeft++ {
			float64 overlap
			float64 area

			left := aCell[aaSorted[i][0]].Duplicate()
			right := aCell[aaSorted[i][nCell - 1]].Duplicate()
			for j := 1; j < nCell - 1; j++ {
				if kk < nLeft {
					tree.Union(&left, &aCell[aaSorted[i][j]])
				} else {
					tree.Union(&right, &aCell[aaSorted[i][j]])
				}
			}
			margin += tree.Margin(&left)
			margin += tree.Margin(&right)
			overlap = tree.Overlap(&left, &right, 1, -1)
			area = tree.Area(&left) + tree.Area(&right)
			if nLeft == RTREE_MINCELLS(tree) || overlap < BestOverlap || (overlap == BestOverlap && area < BestArea) {
				BestLeft = nLeft
				BestOverlap = overlap
				BestArea = area
			}
		}

		if i == 0 || margin < BestMargin {
			BestDim = i
			BestMargin = margin
			BestSplit = BestLeft
		}
	}

	BboxLeft = &aCell[aaSorted[BestDim][0]].Duplicate()
	BboxRight = &aCell[aaSorted[BestDim][BestSplit]].Duplicate()
	for i := 0; i < nCell; i++ {
		cell := cells[aaSorted[BestDim][i]]
		if i < BestSplit {
			tree.nodeInsertCell(Left, cell)
			tree.Union(BboxLeft, cell)
		} else {
			tree.nodeInsertCell(Right, cell)
			tree.Union(BboxRight, cell)
		}
	}
	sqlite3_free(aaSorted)
	return SQLITE_OK
}
#endif

#if VARIANT_GUTTMAN_SPLIT
//	Implementation of the regular R-tree SplitNode from Guttman[1984].
func (tree *Rtree) splitNodeGuttman(cells []*RtreeCell, Left, Right *RtreeNode) (BboxLeft, BboxRight *RtreeCell, rc int) {
	LeftSeed := 0
	RightSeed := 1
	Used := make([]bool, len(cells))

	LeftSeed, RightSeed := PickSeeds(tree, cells)

	BboxLeft = cells[LeftSeed].Duplicate()
	BboxRight = cells[RightSeed].Duplicate()

	tree.nodeInsertCell(Left, cells[iLeftSeed])
	tree.nodeInsertCell(Right, cells[iRightSeed])
	Used[LeftSeed] = true
	Used[RightSeed] = true

	for i := nCell - 2; i > 0; i-- {
		next := tree.PickNext(Cells, BboxLeft, BboxRight, Used)
		diff := tree.cellGrowth(BboxLeft, next) - tree.cellGrowth(BboxRight, next)
		if (RTREE_MINCELLS(tree) - NCELL(Right) == i) || (diff > 0.0 && (RTREE_MINCELLS(tree) - NCELL(Left) != i)) {
			tree.nodeInsertCell(Right, next)
			tree.Union(BboxRight, next)
		} else {
			pRtree.nodeInsertCell(Left, next)
			pRtree.Union(BboxLeft, next)
		}
	}
	return
}
#endif

func (tree *Rtree) updateMapping(rowid int64, node *RtreeNode, height int) int {
	var xSetMapping	func (*Rtree, int64, int64) int
	if height == 0 {
		xSetMapping = rowidWrite
	} else {
		xSetMapping = parentWrite
	}
	if height > 0 {
		if child := tree.nodeHashLookup(rowid); child != nil {
			tree.nodeRelease(child.pParent)
			node.Reference()
			child.pParent = node
		}
	}
	return xSetMapping(tree, rowid, node.iNode)
}

func (tree *Rtree) SplitNode(node *RtreeNode, cell *RtreeCell, height int) (rc int) {
	var Left, Right				*RtreeNode
	var leftbbox, rightbbox		RtreeCell
	var i						int
	var newCellIsRight			bool

	nCell := NCELL(node)

	//	Allocate an array and populate it with a copy of pCell and all cells from node pLeft. Then zero the original node.
	cells := make([]*RtreeCell, nCell + 1)
	for i := 0; i < nCell; i++ {
		cells[i] = tree.nodeGetCell(node, i)
	}
	used := make([]bool, nCell + 1)
	tree.nodeZero(node)
	cells[nCell] = cell.Duplicate()
	nCell++

	if node.iNode == 1 {
		Right = tree.nodeNew(node)
		Left = tree.nodeNew(node)
		tree.iDepth++;
		node.isDirty = true
		writeInt16(node.zData, tree.iDepth)
	} else {
		Left = node;
		Right = tree.nodeNew(Left.pParent)
		Left.Reference()
	}

	Left.zData = make([]byte, tree.iNodeSize)
	Right.zData = make([]byte, tree.iNodeSize)

	leftbbox, rightbbox, rc = tree.AssignCells(cells, Left, Right)
	if rc != SQLITE_OK {
		goto splitnode_out
	}

	//	Ensure both child nodes have node numbers assigned to them by calling nodeWrite(). Node pRight always needs a node number, as it was created
	//	by nodeNew() above. But node pLeft sometimes already has a node number. In this case avoid the all to nodeWrite().
	if SQLITE_OK != (rc = tree.nodeWrite(Right)) || (Left.iNode == 0 && SQLITE_OK != (rc = tree.nodeWrite(Left))) {
		goto splitnode_out
	}

	rightbbox.iRowid = Right.iNode
	leftbbox.iRowid = Left.iNode

	if node.iNode == 1 {
		if rc = tree.InsertCell(Left.pParent, leftbbox, height + 1); rc != SQLITE_OK {
			goto splitnode_out
		}
	} else {
		pParent := Left.pParent
		var iCell	int
		iCell, rc = tree.nodeParentIndex(Left)
		if rc == SQLITE_OK {
			tree.nodeOverwriteCell(pParent, leftbbox, iCell)
			rc = tree.AdjustTree(pParent, leftbbox)
		}
		if rc != SQLITE_OK {
			goto splitnode_out
		}
	}
	if rc = tree.InsertCell(Right.pParent, rightbbox, height + 1) {
		goto splitnode_out
	}

	for i := 0; i < NCELL(Right); i++ {
		rowid := tree.nodeGetRowid(Right, i)
		rc = tree.updateMapping(rowid, Right, height)
		if rowid == cell.iRowid {
			newCellIsRight = true
		}
		if rc != SQLITE_OK {
			goto splitnode_out
		}
	}

	if node.iNode == 1 {
		for i := 0; i < NCELL(Left); i++ {
			rowid := tree.nodeGetRowid(Left, i)
			rc = tree.updateMapping(rowid, Left, height)
			if rc != SQLITE_OK {
				goto splitnode_out
			}
		}
	} else if !newCellIsRight {
		rc = tree.updateMapping(cell.iRowid, Left, height)
	}

	if rc == SQLITE_OK {
		rc = tree.nodeRelease(Right)
		Right = nil
	}
	if rc==SQLITE_OK {
		rc = tree.nodeRelease(Left)
		Left = nil
	}

splitnode_out:
	tree.nodeRelease(Right)
	tree.nodeRelease(Left)
	return
}

//	If node pLeaf is not the root of the r-tree and its pParent pointer is still NULL, load all ancestor nodes of pLeaf into memory and populate
//	the pLeaf->pParent chain all the way up to the root node.
//
//	This operation is required when a row is deleted (or updated - an update is implemented as a delete followed by an insert). SQLite provides the
//	rowid of the row to delete, which can be used to find the leaf on which the entry resides (argument pLeaf). Once the leaf is located, this 
// function is called to determine its ancestry.
func (tree *Rtree) fixLeafParent(Leaf *RtreeNode) (rc int) {
	for child := Leaf; rc == SQLITE_OK && child.iNode != 1 && child.pParent == nil; {
		rc2 := SQLITE_OK
		sqlite3_bind_int64(tree.pReadParent, 1, child.iNode)
		if rc = tree.pReadParent.Step(); rc == SQLITE_ROW {
			var test	*RtreeNode

			//	Before setting child.pParent, test that we are not creating a loop of references (as we would if, say, child == pParent). We don't
			//	want to do this as it leads to a memory leak when trying to delete the referenced counted node structures.
			parent_node := sqlite3_column_int64(tree.pReadParent, 0)
			for test := Leaf; test != nil && test.iNode != iNode; test = test.pParent {}
			if test == nil {
				child.pParent, rc2 = tree.nodeAcquire(parent_node, nil)
			}
		}
		if rc = tree.pReadParent.Reset(); rc == SQLITE_OK {
			rc = rc2
		}
		if rc == SQLITE_OK && child.pParent == nil {
			rc = SQLITE_CORRUPT_VTAB
		}
		child = child.pParent
	}
	return
}

func (tree *Rtree) removeNode(node *RtreeNode, height int) (rc int) {
	var rc2, iCell	int
	var parent		*RtreeNode

	assert( node.nRef == 1 )

	//	Remove the entry in the parent cell.
	iCell, rc = tree.nodeParentIndex(pNode)
	if rc == SQLITE_OK {
		parent = node.pParent
		node.pParent = nil
		rc = tree.deleteCell(parent, iCell, iHeight + 1)
	}
	rc2 = tree.nodeRelease(parent)
	if rc == SQLITE_OK {
		rc = rc2
	}
	if rc != SQLITE_OK {
		return
	}

	//	Remove the xxx_node entry.
	sqlite3_bind_int64(tree.pDeleteNode, 1, node.iNode)
	tree.pDeleteNode.Step()
	if rc = tree.pDeleteNode.Reset(); rc != SQLITE_OK {
		return
	}

	//	Remove the xxx_parent entry.
	sqlite3_bind_int64(tree.pDeleteParent, 1, node.iNode)
	tree.pDeleteParent.Step()
	if rc = tree.pDeleteParent.Reset(); rc != SQLITE_OK {
		return
	}
  
	//	Remove the node from the in-memory hash table and link it into the Rtree.pDeleted list. Its contents will be re-inserted later on.
	pRtree.nodeHashDelete(node)
	node.iNode = height
	node.pNext = tree.pDeleted
	node.nRef++
	tree.pDeleted = node
	return SQLITE_OK
}

func (tree *Rtree) fixBoundingBox(node *RtreeNode) (rc int) {
	if parent := node.pParent; parent != nil {
		var i	int
		nCell := NCELL(node)
		box := tree.nodeGetCell(node, 0)		//	Bounding box for pNode
		for i := 1; i < nCell; i++ {
			tree.Union(&box, tree.nodeGetCell(node, i))
		}
		box.iRowid = node.iNode
		if i, rc = tree.nodeParentIndex(node); rc == SQLITE_OK {
			tree.nodeOverwriteCell(parent, &box, i)
			rc = tree.fixBoundingBox(parent)
		}
	}
	return
}

//	Delete the cell at index iCell of node pNode. After removing the cell, adjust the r-tree data structure if required.
func (tree *Rtree) deleteCell(node *RtreeNode, cell, height int) (rc int) {
	if rc = tree.fixLeafParent(node); rc == SQLITE_OK {
		//	Remove the cell from the node. This call just moves bytes around the in-memory node image, so it cannot fail.
		tree.nodeDeleteCell(node, cell)

		//	If the node is not the tree root and now has less than the minimum number of cells, remove it from the tree. Otherwise, update the
		//	cell in the parent node so that it tightly contains the updated node.
		parent := node.pParent
		assert( parent != nil || node.iNode == 1 )
		if parent != nil {
			if NCELL(node) < RTREE_MINCELLS(tree) {
				rc = tree.removeNode(node, height)
			} else {
				rc = fixBoundingBox(tree, node)
			}
		}
	}
	return
}

//	Insert cell pCell into node pNode. Node pNode is the head of a subtree iHeight high (leaf nodes have iHeight==0).
func (tree *Rtree) InsertCell(node *RtreeNode, cell *RtreeCell, height int) (rc int) {
	if height > 0 {
		if child := tree.nodeHashLookup(cell.iRowid); child != nil {
			tree.nodeRelease(child.pParent)
			node.Reference()
			child.pParent = node
		}
	}
	if tree.nodeInsertCell(node, cell) {
#if VARIANT_RSTARTREE_REINSERT
		if height <= tree.iReinsertHeight || node.iNode == 1 {
			rc = SplitNode(tree, node, cell, height)
		} else {
			tree.iReinsertHeight = height
			rc = tree.Reinsert(node, cell, height)
		}
#else
		rc = SplitNode(tree, node, cell, height)
#endif
	} else {
		if rc = tree.AdjustTree(node, cell); rc == SQLITE_OK {
			if height == 0 {
				rc = tree.rowidWrite(cell.iRowid, node.iNode)
			} else {
				rc = tree.parentWrite(cell.iRowid, node.iNode)
			}
		}
	}
	return
}

func (tree *Rtree) reinsertNodeContent(node *RtreeNode) (rc int) {
	nCell := NCELL(node)
	for i := 0; rc == SQLITE_OK && i < nCell; i++ {
		var insertion_node	*RtreeNode
		cell := tree.nodeGetCell(node, i)

		//	Find a node to store this cell in. pNode->iNode currently contains the height of the sub-tree headed by the cell.
		insertion_node, rc = tree.ChooseLeaf(&cell, int(node.iNode))
		if rc == SQLITE_OK {
			rc = tree.InsertCell(insertion_node, &cell, int(pNode.iNode))
			if rc2 := tree.nodeRelease(insertion_node); rc == SQLITE_OK {
				rc = rc2
			}
		}
	}
	return
}

//	Select a currently unused rowid for a new r-tree record.
func (tree *Rtree) newRowid() (rowid int64, rc int) {
	sqlite3_bind_null(tree.pWriteRowid, 1)
	sqlite3_bind_null(tree.pWriteRowid, 2)
	tree.pWriteRowid.Step()
	rc = tree.pWriteRowid.Reset()
	rowid = sqlite3_last_insert_rowid(tree.db)
	return
}

//	Remove the entry with rowid=iDelete from the r-tree structure.
func (tree *Rtree) DeleteRowid(rowid int64) (rc int) {
	var Leaf, Root	*RtreeNode
	var cell		int

	if Root, rc = tree.nodeAcquire(1, nil); rc == SQLITE_OK {
		Leaf, rc = tree.findLeafNode(rowid)
	}

	//	Delete the cell in question from the leaf node.
	if rc == SQLITE_OK {
		if cell, rc = tree.nodeRowidIndex(Leaf, rowid); rc == SQLITE_OK {
			rc = tree.deleteCell(Leaf, cell, 0)
		}
		if rc2 := tree.nodeRelease(Leaf); rc == SQLITE_OK {
			rc = rc2
		}
	}

	//	Delete the corresponding entry in the <rtree>_rowid table.
	if rc == SQLITE_OK {
		sqlite3_bind_int64(tree.pDeleteRowid, 1, rowid)
		tree.pDeleteRowid.Step()
		rc = tree.pDeleteRowid.Reset()
	}

	//	Check if the root node now has exactly one child. If so, remove it, schedule the contents of the child for reinsertion and reduce the tree height by one.
	//	This is equivalent to copying the contents of the child into the root node (the operation that Gutman's paper says to perform in this scenario).
	if rc == SQLITE_OK && tree.iDepth > 0 && NCELL(Root) == 1 {
		var child	*RtreeNode
		iChild := tree.nodeGetRowid(Root, 0)
		if rc, child = tree.nodeAcquire(iChild, Root); rc == SQLITE_OK {
			rc = tree.removeNode(child, tree.iDepth - 1)
		}
		if rc2 := tree.nodeRelease(child); rc == SQLITE_OK {
			rc = rc2
		}
		if rc == SQLITE_OK {
			tree.iDepth--
			writeInt16(Root.zData, tree.iDepth)
			Root.isDirty = true
		}
	}

	//	Re-insert the contents of any underfull nodes removed from the tree.
	for Leaf = tree.pDeleted; Leaf; Leaf = tree.pDeleted {
		if rc == SQLITE_OK {
			rc = tree.reinsertNodeContent(Leaf)
		}
		tree.pDeleted = Leaf.pNext
		sqlite3_free(Leaf)
	}

	//	Release the reference to the root node.
	if rc == SQLITE_OK {
		rc = tree.nodeRelease(Root)
	} else {
		tree.nodeRelease(Root)
	}
	return
}

//	Rounding constants for float->float64 conversion.
const(
	RNDTOWARDS	= 1.0 - 1.0 / 8388608.0			//	Round towards zero
	RNDAWAY		= 1.0 + 1.0 / 8388608.0			//	Round away from zero
)

//	Convert an sqlite3_value into an RtreeValue (presumably a float) while taking care to round toward negative or positive, respectively.
func rtreeValueDown(v *sqlite3_value) RtreeValue {
	d := sqlite3_value_float64(v)
	f := float(d)
	if f > d {
		if d < 0 {
			f = float(d * RNDAWAY)
		} else {
			f = float(d * RNDTOWARDS)
		}
	}
	return f
}

func rtreeValueUp(v *sqlite_value) RtreeValue {
	d := sqlite3_value_float64(v)
	f := float(d)
	if f < d {
		if d < 0 {
			f = float(d * RNDTOWARDS)
		} else {
			f = float(d * RNDAWAY)
		}
	}
	return
}

//	The xUpdate method for rtree module virtual tables.
func rtreeUpdate(pVtab *sqlite3_vtab, nData int, azData []sqlite3_value) (rowid int64, rc int) {
	tree := (Rtree *)(pVtab)
	HaveRowid := false

	defer func() {
		tree.Release()
	}()

	tree.Reference()
	assert(nData >= 1)

	//	Constraint handling. A write operation on an r-tree table may return SQLITE_CONSTRAINT for two reasons:
	//
	//		1. A duplicate rowid value, or
	//		2. The supplied data violates the "x2>=x1" constraint.
	//
	//	In the first case, if the conflict-handling mode is REPLACE, then the conflicting row can be removed before proceeding. In the second
	//	case, SQLITE_CONSTRAINT must be returned regardless of the conflict-handling mode specified by the user.
	cell := tree.NewCell(-1)				//	New cell to insert if nData > 1
	if nData > 1 {
		//	Populate the cell.aCoord[] array. The first coordinate is azData[3].
		assert( nData == (tree.Dimensions * 2 + 3) )
		for i := 0; i < (tree.Dimensions * 2); i += 2 {
			cell.aCoord[i].i = sqlite3_value_int(azData[i + 3])
			cell.aCoord[i + 1].i = sqlite3_value_int(azData[i + 4])
			if cell.aCoord[i].i > cell.aCoord[i + 1].i {
				rc = SQLITE_CONSTRAINT
				return
			}
		}

		//	If a rowid value was supplied, check if it is already present in the table. If so, the constraint has failed.
		if sqlite3_value_type(azData[2]) != SQLITE_NULL {
			cell.iRowid = sqlite3_value_int64(azData[2])
			if sqlite3_value_type(azData[0]) == SQLITE_NULL || sqlite3_value_int64(azData[0]) != cell.iRowid {
				sqlite3_bind_int64(tree.pReadRowid, 1, cell.iRowid)
				steprc := tree.pReadRowid.Step()
				rc = tree.pReadRowid.Reset()
				if steprc == SQLITE_ROW {
					if sqlite3_vtab_on_conflict(tree.db) == SQLITE_REPLACE {
						rc = tree.DeleteRowid(cell.iRowid)
					} else {
						rc = SQLITE_CONSTRAINT
						return
					}
				}
			}
			HaveRowid = true
		}
	}

	//	If azData[0] is not an SQL NULL value, it is the rowid of a record to delete from the r-tree table. The following block does just that.
	if sqlite3_value_type(azData[0]) != SQLITE_NULL {
		rc = tree.DeleteRowid(sqlite3_value_int64(azData[0]))
	}

	//	If the azData[] array contains more than one element, elements (azData[2]..azData[argc-1]) contain a new record to insert into the r-tree structure.
	if rc == SQLITE_OK && nData > 1 {
		//	Insert the new record into the r-tree
		var Leaf	*RtreeNode

		if !HaveRowid {
			rc, rowid = tree.newRowid()
		}
		cell.iRowid = rowid

		if rc == SQLITE_OK {
			Leaf, rc = tree.ChooseLeaf(&cell, 0)
		}
		if rc == SQLITE_OK {
			tree.iReinsertHeight = -1
			rc = tree.InsertCell(Leaf, &cell, 0)
			if rc2 := tree.nodeRelease(Leaf); rc == SQLITE_OK {
				rc = rc2
			}
		}
	}
	return
}

//	The xRename method for rtree module virtual tables.
func rtreeRename(pVtab *sqlite3_vtab, new_name string) (rc int) {
	tree := (Rtree *)(pVtab)
	zSql := sqlite3_mprintf(
		"ALTER TABLE %Q.'%q_node'   RENAME TO \"%w_node\"; ALTER TABLE %Q.'%q_parent' RENAME TO \"%w_parent\"; ALTER TABLE %Q.'%q_rowid'  RENAME TO \"%w_rowid\";",
		tree.zDb, tree.zName, new_name,
		tree.zDb, tree.zName, new_name,
		tree.zDb, tree.zName, new_name,
	)
	return sqlite3_exec(tree.db, zSql, 0, 0, 0)
}

static sqlite3_module rtreeModule = {
  0,                          /* iVersion */
  rtreeCreate,                /* xCreate - create a table */
  rtreeConnect,               /* xConnect - connect to an existing table */
  rtreeBestIndex,             /* xBestIndex - Determine search strategy */
  rtreeDisconnect,            /* xDisconnect - Disconnect from a table */
  rtreeDestroy,               /* xDestroy - Drop a table */
  rtreeOpen,                  /* xOpen - open a cursor */
  rtreeClose,                 /* xClose - close a cursor */
  rtreeFilter,                /* xFilter - configure scan constraints */
  rtreeNext,                  /* xNext - advance a cursor */
  rtreeEof,                   /* xEof */
  rtreeColumn,                /* xColumn - read data */
  rtreeRowid,                 /* xRowid - read data */
  rtreeUpdate,                /* xUpdate - write data */
  0,                          /* xBegin - begin transaction */
  0,                          /* xSync - sync transaction */
  0,                          /* xCommit - commit transaction */
  0,                          /* xRollback - rollback transaction */
  0,                          /* xFindFunction - function overloading */
  rtreeRename,                /* xRename - rename the table */
  0,                          /* xSavepoint */
  0,                          /* xRelease */
  0                           /* xRollbackTo */
};

const N_STATEMENT = 9

func (tree *Rtree) SqlInit(db *sqlite3, zDb, zPrefix string, isCreate bool) (rc int) {
#define N_STATEMENT 9
	sql_statements := []string{
		//	Read and write the xxx_node table
		"SELECT data FROM '%q'.'%q_node' WHERE nodeno = :1",
		"INSERT OR REPLACE INTO '%q'.'%q_node' VALUES(:1, :2)",
		"DELETE FROM '%q'.'%q_node' WHERE nodeno = :1",

		//	Read and write the xxx_rowid table
		"SELECT nodeno FROM '%q'.'%q_rowid' WHERE rowid = :1",
		"INSERT OR REPLACE INTO '%q'.'%q_rowid' VALUES(:1, :2)",
		"DELETE FROM '%q'.'%q_rowid' WHERE rowid = :1",

		//	Read and write the xxx_parent table
		"SELECT parentnode FROM '%q'.'%q_parent' WHERE nodeno = :1",
		"INSERT OR REPLACE INTO '%q'.'%q_parent' VALUES(:1, :2)",
		"DELETE FROM '%q'.'%q_parent' WHERE nodeno = :1"
	}

	tree.db = db
	if isCreate {
		create_statement := sqlite3_mprintf(
			"CREATE TABLE \"%w\".\"%w_node\"(nodeno INTEGER PRIMARY KEY, data BLOB); CREATE TABLE \"%w\".\"%w_rowid\"(rowid INTEGER PRIMARY KEY, nodeno INTEGER); CREATE TABLE \"%w\".\"%w_parent\"(nodeno INTEGER PRIMARY KEY, parentnode INTEGER); INSERT INTO '%q'.'%q_node' VALUES(1, zeroblob(%d))",
			zDb, zPrefix, zDb, zPrefix, zDb, zPrefix, zDb, zPrefix, tree.iNodeSize
		)
		if rc = sqlite3_exec(db, create_statement, 0, 0, 0); rc != SQLITE_OK {
			return
		}
	}

	appStmt := []sqlite3_stmt{
		tree.pReadNode, tree.pWriteNode, tree.pDeleteNode,
		tree.pReadRowid, tree.pWriteRowid, tree.pDeleteRowid,
		tree.pReadParent, tree.pWriteParent, tree.pDeleteParent,
	}

	for i := 0; i < N_STATEMENT && rc == SQLITE_OK; i++ {
		appStmt[i], _, rc = db.Prepare_v2(sqlite3_mprintf(sql_statements[i], zDb, zPrefix))
	}
	return
}

//	The second argument to this function contains the text of an SQL statement that returns a single integer value. The statement is compiled and executed
//	using database connection db. If successful, the integer value returned is written to *piVal and SQLITE_OK returned. Otherwise, an SQLite error
//	code is returned and the value of *piVal after returning is not defined.
func (db *sqlite3) func GetIntFromStmt(sql string) (val, rc int) {
	var statement	*sqlite3_stmt
	if statement, _, rc = db.Prepare_v2(sql); rc == SQLITE_OK {
		if statement.Step() == SQLITE_ROW {
			val = sqlite3_column_int(statement, 0)
		}
		rc = statement.Finalize()
	}
	return
}

//	This function is called from within the xConnect() or xCreate() method to determine the node-size used by the rtree table being created or connected
//	to. If successful, pRtree->iNodeSize is populated and SQLITE_OK returned. Otherwise, an SQLite error code is returned.
//
//	If this function is being called as part of an xConnect(), then the rtree table already exists. In this case the node-size is determined by inspecting
//	the root node of the tree.
//
//	Otherwise, for an xCreate(), use 64 bytes less than the database page-size. This ensures that each node is stored on a single database page. If the 
//	database page-size is so large that more than RTREE_MAXCELLS entries would fit in a single node, use a smaller node-size.
func (tree *Rtree) getNodeSize(db *sqlite3, isCreate bool) (Err string, rc int) {
	if isCreate {
		sql := sqlite3_mprintf("PRAGMA %Q.page_size", tree.zDb)
		if iPageSize, rc := db.GetIntFromStmt(sql); rc == SQLITE_OK {
			tree.iNodeSize = iPageSize - 64
			if (4 + tree.nBytesPerCell * RTREE_MAXCELLS) < tree.iNodeSize {
				tree.iNodeSize = 4 + tree.nBytesPerCell * RTREE_MAXCELLS
			}
		} else {
			return sqlite3_mprintf("%s", sqlite3_errmsg(db)), rc
		}
	} else {
		sql := sqlite3_mprintf("SELECT length(data) FROM '%q'.'%q_node' WHERE nodeno = 1", tree.zDb, tree.zName)
		if pRtree.iNodeSize, rc = db.GetIntFromStmt(sql); rc != SQLITE_OK {
			Err = sqlite3_mprintf("%s", sqlite3_errmsg(db))
		}
	}
	return
}

//	This function is the implementation of both the xConnect and xCreate methods of the r-tree virtual table.
//
//		argv[0]   -> module name
//		argv[1]   -> database name
//		argv[2]   -> table name
//		argv[...] -> column names...
//
func rtreeInit(db *sqlite3, aux interface{}, args []string, isCreate bool) (table *sqlite3_vtab, Err string, rc int) {
	var tree	*Rtree
	int eCoordType = (pAux ? RTREE_COORD_INT32 : RTREE_COORD_REAL32);

	error_messages := []string{
		"",
		"Wrong number of columns for an rtree table",
		"Too few columns for an rtree table",
		"Too many columns for an rtree table"
	}

	var iErr	int
	switch {
	case len(args) < 6:
		iErr = 2
	case len(args) > (RTREE_MAX_DIMENSIONS * 2 + 4):
		iErr = 3
	default:
		iErr = len(args) % 2
	}
	if error_messages[iErr] {
		Err = sqlite3_mprintf("%s", error_messages[iErr])
		return SQLITE_ERROR
	}

	sqlite3_vtab_config(db, SQLITE_VTAB_CONSTRAINT_SUPPORT, 1)

	//	Allocate the sqlite3_vtab structure
	tree = &Rtree{
		nBusy:			1,
		base.pModule:	&rtreeModule,
		zDb:			args[1],
		zName:			args[2],
		Dimensions:			(len(args) - 4) / 2,
		nBytesPerCell:	8 + tree.Dimensions * 4 * 2,
		eCoordType:		eCoordType,
	}

	//	Figure out the node size to use.
	Err, rc = tree.getNodeSize(db, isCreate)

	//	Create/Connect to the underlying relational database schema. If that is successful, call sqlite3_declare_vtab() to configure the r-tree table schema.
	if rc == SQLITE_OK {
		if rc = tree.SqlInit(db, args[1], args[2], isCreate); rc != SQLITE_OK {
			Err = sqlite3_mprintf("%s", sqlite3_errmsg(db))
		} else {
			sql := sqlite3_mprintf("CREATE TABLE x(%s", args[3])
			for _, v := range args[4:] {
				sql = sqlite3_mprintf("%s, %s", sql, v)
			}
			sql = sqlite3_mprintf("%s);", sql)
			if rc = sqlite3_declare_vtab(db, sql); rc != SQLITE_OK {
				Err = sqlite3_mprintf("%s", sqlite3_errmsg(db))
			}
		}
	}

	if rc == SQLITE_OK {
		table = (sqlite3_vtab *)(tree)
	} else {
		tree.Release()
	}
	return
}

//	Implementation of a scalar function that decodes r-tree nodes to human readable strings. This can be used for debugging and analysis.
//
//	The scalar function takes two arguments, a blob of data containing an r-tree node, and the number of dimensions the r-tree indexes.
//	For a two-dimensional r-tree structure called "rt", to deserialize all nodes, a statement like:
//
//		SELECT rtreenode(2, data) FROM rt_node
//
//	The human readable string takes the form of a Tcl list with one entry for each cell in the r-tree node. Each entry is itself a
//	list, containing the 8-byte rowid/pageno followed by the <num-dimension> * 2 coordinates.
func rtreenode(context *sqlite3_context, args []*sqlite3_value) {
	tree := &Rtree{ Dimensions: sqlite3_value_int(args[0]) }
	tree.nBytesPerCell = 8 + 8 * tree.Dimensions
	node := &RtreeNode{ zData: ([]byte)(sqlite3_value_blob(args[1])) }

	var zText	string
	for i := 0; i < NCELL(node); i++ {
		cell := tree.nodeGetCell(node, i)
		zCell := fmt.Sprintf("%lld", cell.iRowid)
		for j := 0; j < tree.Dimensions * 2; j++ {
			zCell = fmt.Sprintf("%s %f", zCell, float64(cell.aCoord[j].f))
		}

		if len(zText) == 0 {
			zText = fmt.Sprintf("{%s}", zCell)
		} else {
			zText = fmt.Sprintf("%s {%s}", zText, zCell)
		}
	}
	sqlite3_result_text(context, zText, -1, sqlite3_free)
}

func rtreedepth(context *sqlite3_context, args []*sqlite3_value) {
	if sqlite3_value_type(args[0]) != SQLITE_BLOB || sqlite3_value_bytes(args[0]) < 2 {
		sqlite3_result_error(context, "Invalid argument to rtreedepth()", -1)
	} else {
		zBlob := ([]byte)(sqlite3_value_blob(args[0]))
		sqlite3_result_int(context, readInt16(zBlob))
	}
}

//	Register the r-tree module with database handle db. This creates the virtual table module "rtree" and the debugging/analysis scalar function "rtreenode".
func sqlite3RtreeInit(db *sqlite3) (rc int) {
	if rc = sqlite3_create_function(db, "rtreenode", 2, 0, rtreenode, 0, 0); rc == SQLITE_OK {
		if rc = sqlite3_create_function(db, "rtreedepth", 1, 0,rtreedepth, 0, 0); rc == SQLITE_OK {
			void *c = (void *)RTREE_COORD_REAL32
			if rc = sqlite3_create_module_v2(db, "rtree", &rtreeModule, c, 0); rc == SQLITE_OK {
				void *c = (void *)RTREE_COORD_INT32
				rc = sqlite3_create_module_v2(db, "rtree_i32", &rtreeModule, c, 0)
			}
		}
	}
	return
}

//	A version of sqlite3_free() that can be used as a callback. This is used in two places - as the destructor for the blob value returned by the
//	invocation of a geometry function, and as the destructor for the geometry functions themselves.
func doSqlite3Free(p interface{}) {
	sqlite3_free(p)
}


//	Each call to sqlite3_rtree_geometry_callback() creates an ordinary SQLite scalar user function. This C function is the callback used for all such
//	registered SQL functions.
//	The scalar user functions return a blob that is interpreted by r-tree table MATCH operators.
func geomCallback(context *sqlite3_context, args []*sqlite3_value) {
	pGeomCtx := sqlite3_user_data(context).(*RtreeGeomCallback)

	nBlob := sizeof(RtreeMatchArg) + (nArg-1)*sizeof(float64)

	pBlob := &RtreeMatchArg{
		magic: RTREE_GEOMETRY_MAGIC,
		xGeom: pGeomCtx.xGeom,
		pContext: pGeomCtx.pContext,
		nParam: nArg,
	}
	for i := 0; i < nArg; i++ {
		pBlob.aParam[i] = sqlite3_value_float64(aArg[i])
	}
	context.sqlite3_result_blob(pBlob, doSqlite3Free)
}

//	Register a new geometry function for use with the r-tree MATCH operator.
func sqlite3_rtree_geometry_callback(db *sqlite3, Geometry string, xGeom func (*sqlite3_rtree_geometry, int, []float64), Context interface{}) (bool, int) {
	//	Allocate and populate the context object.
	pGeomCtx := &RtreeGeomCallback{ xGeom: xGeom, pContext: Context }

	//	Create the new user-function. Register a destructor function to delete the context object when it is no longer required.
	return sqlite3_create_function_v2(db, Geometry, -1, pGeomCtx, geomCallback, 0, 0, doSqlite3Free)
}

func (tree *Rtree) Reinsert(node *RtreeNode, cell *RtreeCell, height int) (rc int) {
	centre_coordinates := make([]float64, RTREE_MAX_DIMENSIONS)
	cell_count := NCELL(node) + 1
	n := (cell_count + 1) & (~1)

	//	Allocate the buffers used by this operation. The allocation is relinquished before this function returns.
	cells := make([]*RtreeCell, n)
	orders := make([]int, n)
	spares := make([]int, n)
	distances := make([]float64, n)

	var i	int
	for i = cell_count - 1; i > -1; i-- {
		if i == cell_count - 1 {
			copy(&cells[i], cell)
		} else {
			cells[i] = tree.nodeGetCell(node, i)
		}
		orders[i] = i
		for dimension := tree.Dimensions - 1; dimension > -1; dimension-- {
			centre_coordinates[dimension] += DCOORD(cells[i].aCoord[dimension * 2])
			centre_coordinates[dimension] += DCOORD(cells[i].aCoord[dimension * 2 + 1])
		}
	}

	for dimension := tree.Dimensions - 1; dimension > -1; dimension-- {
		centre_coordinates[dimension] = (centre_coordinates[dimension] / (cell_count * 2))
	}

	for i = cell_count - 1; i > -1; i-- {
		for dimension := tree.Dimensions - 1; dimension > -1; dimension-- {
			coord := DCOORD(cells[i].aCoord[dimension * 2 + 1]) - DCOORD(cells[i].aCoord[dimension * 2])
			distances[i] += (coord - centre_coordinates[dimension]) * (coord - centre_coordinates[dimension])
		}
	}

	SortByDistance(orders[:cell_count], distances, spares)
	tree.nodeZero(node)

	for i = 0; rc == SQLITE_OK && i < (cell_count - (RTREE_MINCELLS(tree) + 1)); i++ {
		p := &cells[orders[i]]
		tree.nodeInsertCell(node, p)
		if p.iRowid == cell.iRowid {
			if height == 0 {
				rc = tree.rowidWrite(p.iRowid, node.iNode)
			} else {
				rc = tree.parentWrite(p.iRowid, node.iNode)
			}
		}
	}

	if rc == SQLITE_OK {
		rc = fixBoundingBox(tree, node)
	}

	for ; rc == SQLITE_OK && i < cell_count; i++ {
		//	Find a node to store this cell in. node.iNode currently contains the height of the sub-tree headed by the cell.
		var free_node	*RtreeNode

		p := &cells[orders[i]]
		free_node, rc = tree.ChooseLeaf(p, height)
		if rc == SQLITE_OK {
			rc = tree.InsertCell(free_node, p, height)
			rc2 := tree.nodeRelease(free_node)
			if rc == SQLITE_OK {
				rc = rc2
			}
		}
	}
	return
}