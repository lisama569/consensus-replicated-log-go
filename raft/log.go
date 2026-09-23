package raft

// 日志在内存中的布局：
//
//	n.log[0] 是快照占位项（Index=snapMeta.LastIncludedIndex,
//	Term=snapMeta.LastIncludedTerm）；没有快照时占位项为 {Index:0,Term:0}。
//	n.log[1:] 是快照之后的实际日志后缀。
//
// 所有函数都只允许在节点事件循环内调用。

// physicalIndex 把日志索引换算为 n.log 切片下标。
// 占位项（snapMeta.LastIncludedIndex）恒在下标 0；
// 无快照时占位项是虚拟索引 0，因此真实日志索引 i 对应下标 i。
func (n *Node) physicalIndex(index Index) int {
	return int(index - n.snapMeta.LastIncludedIndex)
}

// entryAt 取指定索引的日志项；index 等于快照最后索引时返回占位项。
func (n *Node) entryAt(index Index) (LogEntry, bool) {
	if index < n.snapMeta.LastIncludedIndex {
		return LogEntry{}, false
	}
	if index == n.snapMeta.LastIncludedIndex {
		if index == 0 {
			// 无快照时的虚拟哨兵 {0,0}。
			return LogEntry{}, true
		}
		return LogEntry{Index: index, Term: n.snapMeta.LastIncludedTerm}, true
	}
	i := n.physicalIndex(index)
	if i < 1 || i >= len(n.log) {
		return LogEntry{}, false
	}
	return n.log[i], true
}

func (n *Node) lastLogIndex() Index {
	if len(n.log) > 0 {
		return n.log[len(n.log)-1].Index
	}
	return n.snapMeta.LastIncludedIndex
}

func (n *Node) lastLogTerm() Term {
	if len(n.log) > 0 {
		return n.log[len(n.log)-1].Term
	}
	return n.snapMeta.LastIncludedTerm
}

func (n *Node) termAt(index Index) Term {
	if e, ok := n.entryAt(index); ok {
		return e.Term
	}
	return 0
}

// entriesAfter 返回索引严格大于 index 的日志项（用于 AppendEntries 携带）。
func (n *Node) entriesAfter(index Index) []LogEntry {
	if index < n.snapMeta.LastIncludedIndex {
		// 请求方落后到快照之前：调用方应改用 InstallSnapshot。
		return nil
	}
	i := n.physicalIndex(index) + 1
	if i < 1 {
		i = 1
	}
	if i >= len(n.log) {
		return nil
	}
	return append([]LogEntry(nil), n.log[i:]...)
}

// appendSuffix 追加日志（要求索引连续），并整体持久化日志后缀。
func (n *Node) appendSuffix(entries []LogEntry) error {
	n.log = append(n.log, entries...)
	return n.persistLog()
}

// truncateTail 删除从 index 起（含）的所有日志项。
func (n *Node) truncateTail(index Index) error {
	if index <= n.snapMeta.LastIncludedIndex {
		return ErrCompacted
	}
	i := n.physicalIndex(index)
	if i < 1 {
		i = 1
	}
	if i >= len(n.log) {
		return nil
	}
	n.log = append([]LogEntry(nil), n.log[:i]...)
	return n.persistLog()
}

// persistLog 将占位项之外的日志后缀整体落盘。
func (n *Node) persistLog() error {
	suffix := []LogEntry{}
	if len(n.log) > 1 {
		suffix = n.log[1:]
	}
	return n.store.SaveLogSuffix(suffix)
}
