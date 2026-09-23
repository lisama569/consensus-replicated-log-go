package raft

// Log 维护带快照基点的日志序列。下标语义：
//   - baseIndex：快照包含的最后一条下标（0 表示无快照）
//   - 有效下标范围：(baseIndex, baseIndex+len(entries)]
//
// 不变式：
//   - entries[i].Index == baseIndex + i + 1
//   - entries 内任期单调不减
type Log struct {
	baseIndex Index
	baseTerm  Term
	entries   []Entry
}

func NewLog() *Log { return &Log{} }

// LastIndex 返回最后一条日志下标（可能来自快照）。
func (l *Log) LastIndex() Index { return l.baseIndex + Index(len(l.entries)) }

// LastTerm 返回最后一条日志任期。
func (l *Log) LastTerm() Term {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Term
	}
	return l.baseTerm
}

// TermAt 返回指定下标的任期；下标为 0 时返回 0。
func (l *Log) TermAt(i Index) (Term, error) {
	if i == 0 {
		return 0, nil
	}
	if i == l.baseIndex {
		return l.baseTerm, nil
	}
	if i < l.baseIndex {
		return 0, ErrCompacted
	}
	off := i - l.baseIndex - 1
	if off >= Index(len(l.entries)) {
		return 0, ErrUnavailable
	}
	return l.entries[off].Term, nil
}

// EntryAt 返回指定下标的日志项。
func (l *Log) EntryAt(i Index) (Entry, error) {
	if i <= l.baseIndex {
		return Entry{}, ErrCompacted
	}
	off := i - l.baseIndex - 1
	if off >= Index(len(l.entries)) {
		return Entry{}, ErrUnavailable
	}
	return l.entries[off], nil
}

// Append 在末尾追加日志项，并维护下标不变式。
func (l *Log) Append(es ...Entry) {
	for _, e := range es {
		e.Index = l.LastIndex() + 1
		l.entries = append(l.entries, e)
	}
}

// TruncateSuffix 截掉从 i 开始（含）的所有日志。
func (l *Log) TruncateSuffix(i Index) {
	if i <= l.baseIndex {
		// 不允许截到快照里；保持快照不变。
		l.entries = l.entries[:0]
		return
	}
	off := i - l.baseIndex - 1
	if off < Index(len(l.entries)) {
		l.entries = l.entries[:off]
	}
}

// Slice 返回 [from, to] 闭区间内的日志副本；越界部分自动裁剪。
func (l *Log) Slice(from, to Index) []Entry {
	if from <= l.baseIndex {
		from = l.baseIndex + 1
	}
	if to > l.LastIndex() {
		to = l.LastIndex()
	}
	if from > to {
		return nil
	}
	out := make([]Entry, to-from+1)
	copy(out, l.entries[from-l.baseIndex-1:to-l.baseIndex])
	return out
}

// CompactTo 将 (baseIndex, i] 的日志压缩掉，i 必须 <= 当前提交位置。
func (l *Log) CompactTo(i Index, term Term) {
	if i <= l.baseIndex {
		return
	}
	if i > l.LastIndex() {
		i = l.LastIndex()
	}
	off := i - l.baseIndex
	l.entries = append([]Entry(nil), l.entries[off:]...)
	l.baseIndex = i
	l.baseTerm = term
}

// ResetToSnapshot 用快照重置日志基点：丢弃快照覆盖范围内的日志，
// 若本地日志在快照点之后仍有内容且与快照点任期一致，则保留后缀。
func (l *Log) ResetToSnapshot(s Snapshot) {
	if s.Index <= l.baseIndex {
		return
	}
	// 若本地恰好有快照点对应的日志且任期一致，保留其后缀；否则清空。
	if t, err := l.TermAt(s.Index); err == nil && t == s.Term {
		off := s.Index - l.baseIndex
		l.entries = append([]Entry(nil), l.entries[off:]...)
	} else {
		l.entries = nil
	}
	l.baseIndex = s.Index
	l.baseTerm = s.Term
}
