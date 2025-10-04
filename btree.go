package main

import "encoding/binary"

// 2 bytes for type, 2 bytes for number of keys
const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

// Node type is just a chunk of bytes
type BNode []byte // can be dumped to the disk

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

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

type BTree struct {
	// pointer (a non-zero page number)
	root uint64
	get  func(uint64) []byte // dereference a pointer (read page from disk)
	new  func([]byte) uint64 // allocate a new page (copy-on-write)
	del  func(uint64)        // deallocate a page
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
