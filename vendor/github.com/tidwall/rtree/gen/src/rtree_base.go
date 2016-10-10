// +build ignore

/*

TITLE

	R-TREES: A DYNAMIC INDEX STRUCTURE FOR SPATIAL SEARCHING

DESCRIPTION

	A Go version of the RTree algorithm.

AUTHORS

	* 1983 Original algorithm and test code by Antonin Guttman and Michael Stonebraker, UC Berkely
	* 1994 ANCI C ported from original test code by Melinda Green - melinda@superliminal.com
	* 1995 Sphere volume fix for degeneracy problem submitted by Paul Brook
	* 2004 Templated C++ port by Greg Douglas
	* 2016 Go port by Josh Baker

LICENSE:

	Entirely free for all uses. Enjoy!

*/

// Implementation of RTree, a multidimensional bounding rectangle tree.
package rtree

import "math"

// FILE_START

func DD_fmin(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
func DD_fmax(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

const (
	DD_numDims            = TNUMDIMS
	DD_maxNodes           = 8
	DD_minNodes           = DD_maxNodes / 2
	DD_useSphericalVolume = true // Better split classification, may be slower on some systems
)

var DD_unitSphereVolume = []float64{
	0.000000, 2.000000, 3.141593, // Dimension  0,1,2
	4.188790, 4.934802, 5.263789, // Dimension  3,4,5
	5.167713, 4.724766, 4.058712, // Dimension  6,7,8
	3.298509, 2.550164, 1.884104, // Dimension  9,10,11
	1.335263, 0.910629, 0.599265, // Dimension  12,13,14
	0.381443, 0.235331, 0.140981, // Dimension  15,16,17
	0.082146, 0.046622, 0.025807, // Dimension  18,19,20
}[DD_numDims]

type DD_RTree struct {
	root *DD_nodeT ///< Root of tree
}

/// Minimal bounding rectangle (n-dimensional)
type DD_rectT struct {
	min [DD_numDims]float64 ///< Min dimensions of bounding box
	max [DD_numDims]float64 ///< Max dimensions of bounding box
}

/// May be data or may be another subtree
/// The parents level determines this.
/// If the parents level is 0, then this is data
type DD_branchT struct {
	rect  DD_rectT    ///< Bounds
	child *DD_nodeT   ///< Child node
	data  interface{} ///< Data Id or Ptr
}

/// DD_nodeT for each branch level
type DD_nodeT struct {
	count  int                     ///< Count
	level  int                     ///< Leaf is zero, others positive
	branch [DD_maxNodes]DD_branchT ///< Branch
}

func (node *DD_nodeT) isInternalNode() bool {
	return (node.level > 0) // Not a leaf, but a internal node
}
func (node *DD_nodeT) isLeaf() bool {
	return (node.level == 0) // A leaf, contains data
}

/// A link list of nodes for reinsertion after a delete operation
type DD_listNodeT struct {
	next *DD_listNodeT ///< Next in list
	node *DD_nodeT     ///< Node
}

const DD_notTaken = -1 // indicates that position

/// Variables for finding a split partition
type DD_partitionVarsT struct {
	partition [DD_maxNodes + 1]int
	total     int
	minFill   int
	count     [2]int
	cover     [2]DD_rectT
	area      [2]float64

	branchBuf      [DD_maxNodes + 1]DD_branchT
	branchCount    int
	coverSplit     DD_rectT
	coverSplitArea float64
}

func DD_New() *DD_RTree {
	// We only support machine word size simple data type eg. integer index or object pointer.
	// Since we are storing as union with non data branch
	return &DD_RTree{
		root: &DD_nodeT{},
	}
}

/// Insert entry
/// \param a_min Min of bounding rect
/// \param a_max Max of bounding rect
/// \param a_dataId Positive Id of data.  Maybe zero, but negative numbers not allowed.
func (tr *DD_RTree) Insert(min, max [DD_numDims]float64, dataId interface{}) {
	var branch DD_branchT
	branch.data = dataId
	for axis := 0; axis < DD_numDims; axis++ {
		branch.rect.min[axis] = min[axis]
		branch.rect.max[axis] = max[axis]
	}
	DD_insertRect(&branch, &tr.root, 0)
}

/// Remove entry
/// \param a_min Min of bounding rect
/// \param a_max Max of bounding rect
/// \param a_dataId Positive Id of data.  Maybe zero, but negative numbers not allowed.
func (tr *DD_RTree) Remove(min, max [DD_numDims]float64, dataId interface{}) {
	var rect DD_rectT
	for axis := 0; axis < DD_numDims; axis++ {
		rect.min[axis] = min[axis]
		rect.max[axis] = max[axis]
	}
	DD_removeRect(&rect, dataId, &tr.root)
}

/// Find all within DD_search rectangle
/// \param a_min Min of DD_search bounding rect
/// \param a_max Max of DD_search bounding rect
/// \param a_searchResult DD_search result array.  Caller should set grow size. Function will reset, not append to array.
/// \param a_resultCallback Callback function to return result.  Callback should return 'true' to continue searching
/// \param a_context User context to pass as parameter to a_resultCallback
/// \return Returns the number of entries found
func (tr *DD_RTree) Search(min, max [DD_numDims]float64, resultCallback func(data interface{}) bool) int {
	var rect DD_rectT
	for axis := 0; axis < DD_numDims; axis++ {
		rect.min[axis] = min[axis]
		rect.max[axis] = max[axis]
	}
	foundCount, _ := DD_search(tr.root, rect, 0, resultCallback)
	return foundCount
}

/// Count the data elements in this container.  This is slow as no internal counter is maintained.
func (tr *DD_RTree) Count() int {
	var count int
	DD_countRec(tr.root, &count)
	return count
}

/// Remove all entries from tree
func (tr *DD_RTree) RemoveAll() {
	// Delete all existing nodes
	tr.root = &DD_nodeT{}
}

func DD_countRec(node *DD_nodeT, count *int) {
	if node.isInternalNode() { // not a leaf node
		for index := 0; index < node.count; index++ {
			DD_countRec(node.branch[index].child, count)
		}
	} else { // A leaf node
		*count += node.count
	}
}

// Inserts a new data rectangle into the index structure.
// Recursively descends tree, propagates splits back up.
// Returns 0 if node was not split.  Old node updated.
// If node was split, returns 1 and sets the pointer pointed to by
// new_node to point to the new node.  Old node updated to become one of two.
// The level argument specifies the number of steps up from the leaf
// level to insert; e.g. a data rectangle goes in at level = 0.
func DD_insertRectRec(branch *DD_branchT, node *DD_nodeT, newNode **DD_nodeT, level int) bool {
	// recurse until we reach the correct level for the new record. data records
	// will always be called with a_level == 0 (leaf)
	if node.level > level {
		// Still above level for insertion, go down tree recursively
		var otherNode *DD_nodeT
		//var newBranch DD_branchT

		// find the optimal branch for this record
		index := DD_pickBranch(&branch.rect, node)

		// recursively insert this record into the picked branch
		childWasSplit := DD_insertRectRec(branch, node.branch[index].child, &otherNode, level)

		if !childWasSplit {
			// Child was not split. Merge the bounding box of the new record with the
			// existing bounding box
			node.branch[index].rect = DD_combineRect(&branch.rect, &(node.branch[index].rect))
			return false
		} else {
			// Child was split. The old branches are now re-partitioned to two nodes
			// so we have to re-calculate the bounding boxes of each node
			node.branch[index].rect = DD_nodeCover(node.branch[index].child)
			var newBranch DD_branchT
			newBranch.child = otherNode
			newBranch.rect = DD_nodeCover(otherNode)

			// The old node is already a child of a_node. Now add the newly-created
			// node to a_node as well. a_node might be split because of that.
			return DD_addBranch(&newBranch, node, newNode)
		}
	} else if node.level == level {
		// We have reached level for insertion. Add rect, split if necessary
		return DD_addBranch(branch, node, newNode)
	} else {
		// Should never occur
		return false
	}
}

// Insert a data rectangle into an index structure.
// DD_insertRect provides for splitting the root;
// returns 1 if root was split, 0 if it was not.
// The level argument specifies the number of steps up from the leaf
// level to insert; e.g. a data rectangle goes in at level = 0.
// InsertRect2 does the recursion.
//
func DD_insertRect(branch *DD_branchT, root **DD_nodeT, level int) bool {
	var newNode *DD_nodeT

	if DD_insertRectRec(branch, *root, &newNode, level) { // Root split

		// Grow tree taller and new root
		newRoot := &DD_nodeT{}
		newRoot.level = (*root).level + 1

		var newBranch DD_branchT

		// add old root node as a child of the new root
		newBranch.rect = DD_nodeCover(*root)
		newBranch.child = *root
		DD_addBranch(&newBranch, newRoot, nil)

		// add the split node as a child of the new root
		newBranch.rect = DD_nodeCover(newNode)
		newBranch.child = newNode
		DD_addBranch(&newBranch, newRoot, nil)

		// set the new root as the root node
		*root = newRoot

		return true
	}
	return false
}

// Find the smallest rectangle that includes all rectangles in branches of a node.
func DD_nodeCover(node *DD_nodeT) DD_rectT {
	rect := node.branch[0].rect
	for index := 1; index < node.count; index++ {
		rect = DD_combineRect(&rect, &(node.branch[index].rect))
	}
	return rect
}

// Add a branch to a node.  Split the node if necessary.
// Returns 0 if node not split.  Old node updated.
// Returns 1 if node split, sets *new_node to address of new node.
// Old node updated, becomes one of two.
func DD_addBranch(branch *DD_branchT, node *DD_nodeT, newNode **DD_nodeT) bool {
	if node.count < DD_maxNodes { // Split won't be necessary
		node.branch[node.count] = *branch
		node.count++
		return false
	} else {
		DD_splitNode(node, branch, newNode)
		return true
	}
}

// Disconnect a dependent node.
// Caller must return (or stop using iteration index) after this as count has changed
func DD_disconnectBranch(node *DD_nodeT, index int) {
	// Remove element by swapping with the last element to prevent gaps in array
	node.branch[index] = node.branch[node.count-1]
	node.branch[node.count-1].data = nil
	node.branch[node.count-1].child = nil
	node.count--
}

// Pick a branch.  Pick the one that will need the smallest increase
// in area to accomodate the new rectangle.  This will result in the
// least total area for the covering rectangles in the current node.
// In case of a tie, pick the one which was smaller before, to get
// the best resolution when searching.
func DD_pickBranch(rect *DD_rectT, node *DD_nodeT) int {
	var firstTime bool = true
	var increase float64
	var bestIncr float64 = -1
	var area float64
	var bestArea float64
	var best int
	var tempRect DD_rectT

	for index := 0; index < node.count; index++ {
		curRect := &node.branch[index].rect
		area = DD_calcRectVolume(curRect)
		tempRect = DD_combineRect(rect, curRect)
		increase = DD_calcRectVolume(&tempRect) - area
		if (increase < bestIncr) || firstTime {
			best = index
			bestArea = area
			bestIncr = increase
			firstTime = false
		} else if (increase == bestIncr) && (area < bestArea) {
			best = index
			bestArea = area
			bestIncr = increase
		}
	}
	return best
}

// Combine two rectangles into larger one containing both
func DD_combineRect(rectA, rectB *DD_rectT) DD_rectT {
	var newRect DD_rectT

	for index := 0; index < DD_numDims; index++ {
		newRect.min[index] = DD_fmin(rectA.min[index], rectB.min[index])
		newRect.max[index] = DD_fmax(rectA.max[index], rectB.max[index])
	}

	return newRect
}

// Split a node.
// Divides the nodes branches and the extra one between two nodes.
// Old node is one of the new ones, and one really new one is created.
// Tries more than one method for choosing a partition, uses best result.
func DD_splitNode(node *DD_nodeT, branch *DD_branchT, newNode **DD_nodeT) {
	// Could just use local here, but member or external is faster since it is reused
	var localVars DD_partitionVarsT
	parVars := &localVars

	// Load all the branches into a buffer, initialize old node
	DD_getBranches(node, branch, parVars)

	// Find partition
	DD_choosePartition(parVars, DD_minNodes)

	// Create a new node to hold (about) half of the branches
	*newNode = &DD_nodeT{}
	(*newNode).level = node.level

	// Put branches from buffer into 2 nodes according to the chosen partition
	node.count = 0
	DD_loadNodes(node, *newNode, parVars)
}

// Calculate the n-dimensional volume of a rectangle
func DD_rectVolume(rect *DD_rectT) float64 {
	var volume float64 = 1
	for index := 0; index < DD_numDims; index++ {
		volume *= rect.max[index] - rect.min[index]
	}
	return volume
}

// The exact volume of the bounding sphere for the given DD_rectT
func DD_rectSphericalVolume(rect *DD_rectT) float64 {
	var sumOfSquares float64 = 0
	var radius float64

	for index := 0; index < DD_numDims; index++ {
		halfExtent := (rect.max[index] - rect.min[index]) * 0.5
		sumOfSquares += halfExtent * halfExtent
	}

	radius = math.Sqrt(sumOfSquares)

	// Pow maybe slow, so test for common dims just use x*x, x*x*x.
	if DD_numDims == 5 {
		return (radius * radius * radius * radius * radius * DD_unitSphereVolume)
	} else if DD_numDims == 4 {
		return (radius * radius * radius * radius * DD_unitSphereVolume)
	} else if DD_numDims == 3 {
		return (radius * radius * radius * DD_unitSphereVolume)
	} else if DD_numDims == 2 {
		return (radius * radius * DD_unitSphereVolume)
	} else {
		return (math.Pow(radius, DD_numDims) * DD_unitSphereVolume)
	}
}

// Use one of the methods to calculate retangle volume
func DD_calcRectVolume(rect *DD_rectT) float64 {
	if DD_useSphericalVolume {
		return DD_rectSphericalVolume(rect) // Slower but helps certain merge cases
	} else { // RTREE_USE_SPHERICAL_VOLUME
		return DD_rectVolume(rect) // Faster but can cause poor merges
	} // RTREE_USE_SPHERICAL_VOLUME
}

// Load branch buffer with branches from full node plus the extra branch.
func DD_getBranches(node *DD_nodeT, branch *DD_branchT, parVars *DD_partitionVarsT) {
	// Load the branch buffer
	for index := 0; index < DD_maxNodes; index++ {
		parVars.branchBuf[index] = node.branch[index]
	}
	parVars.branchBuf[DD_maxNodes] = *branch
	parVars.branchCount = DD_maxNodes + 1

	// Calculate rect containing all in the set
	parVars.coverSplit = parVars.branchBuf[0].rect
	for index := 1; index < DD_maxNodes+1; index++ {
		parVars.coverSplit = DD_combineRect(&parVars.coverSplit, &parVars.branchBuf[index].rect)
	}
	parVars.coverSplitArea = DD_calcRectVolume(&parVars.coverSplit)
}

// Method #0 for choosing a partition:
// As the seeds for the two groups, pick the two rects that would waste the
// most area if covered by a single rectangle, i.e. evidently the worst pair
// to have in the same group.
// Of the remaining, one at a time is chosen to be put in one of the two groups.
// The one chosen is the one with the greatest difference in area expansion
// depending on which group - the rect most strongly attracted to one group
// and repelled from the other.
// If one group gets too full (more would force other group to violate min
// fill requirement) then other group gets the rest.
// These last are the ones that can go in either group most easily.
func DD_choosePartition(parVars *DD_partitionVarsT, minFill int) {
	var biggestDiff float64
	var group, chosen, betterGroup int

	DD_initParVars(parVars, parVars.branchCount, minFill)
	DD_pickSeeds(parVars)

	for ((parVars.count[0] + parVars.count[1]) < parVars.total) &&
		(parVars.count[0] < (parVars.total - parVars.minFill)) &&
		(parVars.count[1] < (parVars.total - parVars.minFill)) {
		biggestDiff = -1
		for index := 0; index < parVars.total; index++ {
			if DD_notTaken == parVars.partition[index] {
				curRect := &parVars.branchBuf[index].rect
				rect0 := DD_combineRect(curRect, &parVars.cover[0])
				rect1 := DD_combineRect(curRect, &parVars.cover[1])
				growth0 := DD_calcRectVolume(&rect0) - parVars.area[0]
				growth1 := DD_calcRectVolume(&rect1) - parVars.area[1]
				diff := growth1 - growth0
				if diff >= 0 {
					group = 0
				} else {
					group = 1
					diff = -diff
				}

				if diff > biggestDiff {
					biggestDiff = diff
					chosen = index
					betterGroup = group
				} else if (diff == biggestDiff) && (parVars.count[group] < parVars.count[betterGroup]) {
					chosen = index
					betterGroup = group
				}
			}
		}
		DD_classify(chosen, betterGroup, parVars)
	}

	// If one group too full, put remaining rects in the other
	if (parVars.count[0] + parVars.count[1]) < parVars.total {
		if parVars.count[0] >= parVars.total-parVars.minFill {
			group = 1
		} else {
			group = 0
		}
		for index := 0; index < parVars.total; index++ {
			if DD_notTaken == parVars.partition[index] {
				DD_classify(index, group, parVars)
			}
		}
	}
}

// Copy branches from the buffer into two nodes according to the partition.
func DD_loadNodes(nodeA, nodeB *DD_nodeT, parVars *DD_partitionVarsT) {
	for index := 0; index < parVars.total; index++ {
		targetNodeIndex := parVars.partition[index]
		targetNodes := []*DD_nodeT{nodeA, nodeB}

		// It is assured that DD_addBranch here will not cause a node split.
		DD_addBranch(&parVars.branchBuf[index], targetNodes[targetNodeIndex], nil)
	}
}

// Initialize a DD_partitionVarsT structure.
func DD_initParVars(parVars *DD_partitionVarsT, maxRects, minFill int) {
	parVars.count[0] = 0
	parVars.count[1] = 0
	parVars.area[0] = 0
	parVars.area[1] = 0
	parVars.total = maxRects
	parVars.minFill = minFill
	for index := 0; index < maxRects; index++ {
		parVars.partition[index] = DD_notTaken
	}
}

func DD_pickSeeds(parVars *DD_partitionVarsT) {
	var seed0, seed1 int
	var worst, waste float64
	var area [DD_maxNodes + 1]float64

	for index := 0; index < parVars.total; index++ {
		area[index] = DD_calcRectVolume(&parVars.branchBuf[index].rect)
	}

	worst = -parVars.coverSplitArea - 1
	for indexA := 0; indexA < parVars.total-1; indexA++ {
		for indexB := indexA + 1; indexB < parVars.total; indexB++ {
			oneRect := DD_combineRect(&parVars.branchBuf[indexA].rect, &parVars.branchBuf[indexB].rect)
			waste = DD_calcRectVolume(&oneRect) - area[indexA] - area[indexB]
			if waste > worst {
				worst = waste
				seed0 = indexA
				seed1 = indexB
			}
		}
	}

	DD_classify(seed0, 0, parVars)
	DD_classify(seed1, 1, parVars)
}

// Put a branch in one of the groups.
func DD_classify(index, group int, parVars *DD_partitionVarsT) {
	parVars.partition[index] = group

	// Calculate combined rect
	if parVars.count[group] == 0 {
		parVars.cover[group] = parVars.branchBuf[index].rect
	} else {
		parVars.cover[group] = DD_combineRect(&parVars.branchBuf[index].rect, &parVars.cover[group])
	}

	// Calculate volume of combined rect
	parVars.area[group] = DD_calcRectVolume(&parVars.cover[group])

	parVars.count[group]++
}

// Delete a data rectangle from an index structure.
// Pass in a pointer to a DD_rectT, the tid of the record, ptr to ptr to root node.
// Returns 1 if record not found, 0 if success.
// DD_removeRect provides for eliminating the root.
func DD_removeRect(rect *DD_rectT, id interface{}, root **DD_nodeT) bool {
	var reInsertList *DD_listNodeT

	if !DD_removeRectRec(rect, id, *root, &reInsertList) {
		// Found and deleted a data item
		// Reinsert any branches from eliminated nodes
		for reInsertList != nil {
			tempNode := reInsertList.node

			for index := 0; index < tempNode.count; index++ {
				// TODO go over this code. should I use (tempNode->m_level - 1)?
				DD_insertRect(&tempNode.branch[index], root, tempNode.level)
			}
			reInsertList = reInsertList.next
		}

		// Check for redundant root (not leaf, 1 child) and eliminate TODO replace
		// if with while? In case there is a whole branch of redundant roots...
		if (*root).count == 1 && (*root).isInternalNode() {
			tempNode := (*root).branch[0].child
			*root = tempNode
		}
		return false
	} else {
		return true
	}
}

// Delete a rectangle from non-root part of an index structure.
// Called by DD_removeRect.  Descends tree recursively,
// merges branches on the way back up.
// Returns 1 if record not found, 0 if success.
func DD_removeRectRec(rect *DD_rectT, id interface{}, node *DD_nodeT, listNode **DD_listNodeT) bool {
	if node.isInternalNode() { // not a leaf node
		for index := 0; index < node.count; index++ {
			if DD_overlap(*rect, node.branch[index].rect) {
				if !DD_removeRectRec(rect, id, node.branch[index].child, listNode) {
					if node.branch[index].child.count >= DD_minNodes {
						// child removed, just resize parent rect
						node.branch[index].rect = DD_nodeCover(node.branch[index].child)
					} else {
						// child removed, not enough entries in node, eliminate node
						DD_reInsert(node.branch[index].child, listNode)
						DD_disconnectBranch(node, index) // Must return after this call as count has changed
					}
					return false
				}
			}
		}
		return true
	} else { // A leaf node
		for index := 0; index < node.count; index++ {
			if node.branch[index].data == id {
				DD_disconnectBranch(node, index) // Must return after this call as count has changed
				return false
			}
		}
		return true
	}
}

// Decide whether two rectangles DD_overlap.
func DD_overlap(rectA, rectB DD_rectT) bool {
	for index := 0; index < DD_numDims; index++ {
		if rectA.min[index] > rectB.max[index] ||
			rectB.min[index] > rectA.max[index] {
			return false
		}
	}
	return true
}

// Add a node to the reinsertion list.  All its branches will later
// be reinserted into the index structure.
func DD_reInsert(node *DD_nodeT, listNode **DD_listNodeT) {
	newListNode := &DD_listNodeT{}
	newListNode.node = node
	newListNode.next = *listNode
	*listNode = newListNode
}

// DD_search in an index tree or subtree for all data retangles that DD_overlap the argument rectangle.
func DD_search(node *DD_nodeT, rect DD_rectT, foundCount int, resultCallback func(data interface{}) bool) (int, bool) {
	if node.isInternalNode() {
		// This is an internal node in the tree
		for index := 0; index < node.count; index++ {
			if DD_overlap(rect, node.branch[index].rect) {
				var ok bool
				foundCount, ok = DD_search(node.branch[index].child, rect, foundCount, resultCallback)
				if !ok {
					// The callback indicated to stop searching
					return foundCount, false
				}
			}
		}
	} else {
		// This is a leaf node
		for index := 0; index < node.count; index++ {
			if DD_overlap(rect, node.branch[index].rect) {
				id := node.branch[index].data
				foundCount++
				if !resultCallback(id) {
					return foundCount, false // Don't continue searching
				}

			}
		}
	}
	return foundCount, true // Continue searching
}
