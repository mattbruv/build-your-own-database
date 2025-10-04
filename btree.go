package main

import (
	"bytes"
	"encoding/binary"
)

// 2 bytes for type, 2 bytes for number of keys
const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

// Node type is just a chunk of bytes
type BNode []byte // can be dumped to the disk

type BTree struct {
	// pointer (a non-zero page number)
	root uint64
	get  func(uint64) []byte // dereference a pointer (read page from disk)
	new  func([]byte) uint64 // allocate a new page (copy-on-write)
	del  func(uint64)        // deallocate a page
}

const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
)

func (node BNode) btype() uint16 {
	// First two bytes holds node type
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// pointers (each is 8 bytes, or u64)
func (node BNode) getPointer(idx uint16) uint64 {
	assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPointer(idx uint16, val uint64)

// offset list
// offsets begin after pointer list
func offsetPos(node BNode, idx uint16) uint16 {
	assert(idx > 1 && idx <= node.nkeys())
	pointerSectionSize := 8 * node.nkeys()
	return HEADER + pointerSectionSize + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16)

// key-values

// return the position of the nth KV pair relative to the whole node
func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys())
	pointerSectionSize := 8 * node.nkeys()
	return HEADER + pointerSectionSize + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte

// returns the node size (used space) with an off-by-one lookup
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// returns the first kid node who's range intersects the key. (kid[i] <= key)
// Node lookup (Less than or Equal)
// TODO: binary search
func nodeLookupLE(node BNode, key []byte) uint16 {
	found := uint16(0)

	// the first key is a copy from the parent node
	// thus it's always less than or equal to the key
	for i := uint16(1); i < node.nkeys(); i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		// The result will be 0 if a == b,
		// -1 if a < b,
		// +1 if a > b.
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}

	return found
}

// add a new key to a leaf node
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1) // set up the header
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// Copies a KV pair
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPointer(idx, ptr)

	// KVs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))

	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)

	// the offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// copies multiple KVs into the position from the old node
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16)

// replace a link with one or multiple links
func nodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)

	for i, node := range kids {
		// tree.new() callback is used to allocate the child nodes
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
		//                ^ position     ^ pointer       ^ key           ^ val
	}

	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

func initialize() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert(node1max <= BTREE_PAGE_SIZE)
}

func assert(b bool) {
	if !b {
		panic("Assertion failed")
	}
}
