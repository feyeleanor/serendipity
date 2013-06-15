package rtree

const (
	RTREE_MAX_DIMENSIONS	= 5		//	The rtree may have between 1 and RTREE_MAX_DIMENSIONS dimensions.
	HASHSIZE				= 128	//	Size of hash table Rtree.aHash. This hash table is not expected to ever contain very many entries, so a fixed number of buckets is used.
)

//	An rtree virtual-table object.
type Rtree struct {
	base			sqlite3_vtab
	db				*sqlite3			//	Host database connection
	iNodeSize		int					//	Size in bytes of each node in the node table
	Dimensions		int					//	Number of dimensions
	nBytesPerCell	int					//	Bytes consumed per cell
	iDepth			int					//	Current depth of the r-tree structure
	zDb				string				//	Name of database containing r-tree table
	zName			string				//	Name of r-tree table
	aHash			[HASHSIZE]RtreeNode	//	Hash table of in-memory nodes
	nBusy			int					//	Current number of users of this structure

	//	List of nodes removed during a CondenseTree operation. List is linked together via the pointer normally used for hash chains -
	//	RtreeNode.pNext. RtreeNode.iNode stores the depth of the sub-tree headed by the node (leaf nodes have RtreeNode.iNode==0).
	pDeleted		*RtreeNode
	iReinsertHeight	int					//	Height of sub-trees Reinsert() has run on */

	//	Statements to read/write/delete a record from xxx_node
	pReadNode		*sqlite3_stmt
	pWriteNode		*sqlite3_stmt
	pDeleteNode		*sqlite3_stmt

	//	Statements to read/write/delete a record from xxx_rowid
	pReadRowid		*sqlite3_stmt
	pWriteRowid		*sqlite3_stmt
	pDeleteRowid	*sqlite3_stmt

	//	Statements to read/write/delete a record from xxx_parent
	pReadParent		*sqlite3_stmt
	pWriteParent	*sqlite3_stmt
	pDeleteParent	*sqlite3_stmt

	eCoordType		int
}
