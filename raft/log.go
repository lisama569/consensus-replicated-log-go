package raft

// Log 维护带快照基点的日志序列。下标语义：
//   - baseIndex：快照包含的最后一条下标（0 表示无快照）
//   - 有效下标范围：(baseIndex, baseIndex+len(entries)]
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
func (l *Log) TermAt(i Index) (Term, error) { return 0, ErrUnavailable }

// EntryAt 返回指定下标的日志项。
func (l *Log) EntryAt(i Index) (Entry, error) { return Entry{}, ErrUnavailable }

// Append 在末尾追加日志项。
func (l *Log) Append(e ...Entry) {}

// TruncateSuffix 截掉从 i 开始（含）的所有日志。
func (l *Log) TruncateSuffix(i Index) {}

// Slice 返回 [from, to] 闭区间内的日志副本。
func (l *Log) Slice(from, to Index) []Entry { return nil }

// CompactTo 将 (baseIndex, i] 的日志压缩掉，i 必须 <= 当前提交位置。
func (l *Log) CompactTo(i Index, term Term) {}

// ResetToSnapshot 用快照重置日志基点。
func (l *Log) ResetToSnapshot(s Snapshot) {}
