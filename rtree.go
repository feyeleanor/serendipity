package serendipity

func Reinsert(tree *Rtree, node *RtreeNode, cell *RtreeCell, height int) (rc int) {
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
			nodeGetCell(tree, node, i, &cells[i])
		}
		orders[i] = i
		for dimension := tree.nDim - 1; dimension > -1; dimension-- {
			centre_coordinates[dimension] += DCOORD(cells[i].aCoord[dimension * 2])
			centre_coordinates[dimension] += DCOORD(cells[i].aCoord[dimension * 2 + 1])
		}
	}

	for dimension := tree.nDim - 1; dimension > -1; dimension-- {
		centre_coordinates[dimension] = (centre_coordinates[dimension] / (cell_count * 2))
	}

	for i = cell_count - 1; i > -1; i-- {
		for dimension := tree.nDim - 1; dimension > -1; dimension-- {
			coord := DCOORD(cells[i].aCoord[dimension * 2 + 1]) - DCOORD(cells[i].aCoord[dimension * 2])
			distances[i] += (coord - centre_coordinates[dimension]) * (coord - centre_coordinates[dimension])
		}
	}

	SortByDistance(orders, cell_count, distances, spares)
	nodeZero(tree, node)

	for i = 0; rc == SQLITE_OK && i < (cell_count - (RTREE_MINCELLS(tree) + 1)); i++ {
		p := &cells[orders[i]]
		nodeInsertCell(tree, node, p)
		if p.iRowid == cell.iRowid {
			if height == 0 {
				rc = rowidWrite(tree, p.iRowid, node.iNode)
			} else {
				rc = parentWrite(tree, p.iRowid, node.iNode)
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
		rc = ChooseLeaf(tree, p, height, &free_node)
		if rc == SQLITE_OK {
			rc = rtreeInsertCell(tree, free_node, p, height)
			rc2 := nodeRelease(tree, free_node)
			if rc == SQLITE_OK {
				rc = rc2
			}
		}
	}
	return
}