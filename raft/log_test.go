package raft

import "testing"

func TestLogAppendAndIndexInvariant(t *testing.T) {
	l := NewLog()
	l.Append(Entry{Term: 1}, Entry{Term: 1}, Entry{Term: 2})
	if l.LastIndex() != 3 || l.LastTerm() != 2 {
		t.Fatalf("last=(%d,%d), want (3,2)", l.LastIndex(), l.LastTerm())
	}
	for i := Index(1); i <= 3; i++ {
		e, err := l.EntryAt(i)
		if err != nil || e.Index != i {
			t.Fatalf("EntryAt(%d)=%+v,%v", i, e, err)
		}
	}
}

func TestLogTruncateSuffix(t *testing.T) {
	l := NewLog()
	l.Append(Entry{Term: 1}, Entry{Term: 2}, Entry{Term: 3})
	l.TruncateSuffix(2)
	if l.LastIndex() != 1 || l.LastTerm() != 1 {
		t.Fatalf("after truncate last=(%d,%d), want (1,1)", l.LastIndex(), l.LastTerm())
	}
}

func TestLogCompactAndTermAt(t *testing.T) {
	l := NewLog()
	l.Append(Entry{Term: 1}, Entry{Term: 2}, Entry{Term: 3}, Entry{Term: 3})
	l.CompactTo(2, 2)
	if _, err := l.TermAt(1); err != ErrCompacted {
		t.Fatalf("TermAt(1) err=%v, want ErrCompacted", err)
	}
	if tm, err := l.TermAt(2); err != nil || tm != 2 {
		t.Fatalf("TermAt(2)=(%d,%v), want (2,nil)", tm, err)
	}
	if l.LastIndex() != 4 {
		t.Fatalf("LastIndex=%d, want 4", l.LastIndex())
	}
	// 压缩后继续追加，下标必须连续。
	l.Append(Entry{Term: 4})
	e, _ := l.EntryAt(5)
	if e.Index != 5 || e.Term != 4 {
		t.Fatalf("EntryAt(5)=%+v", e)
	}
}

func TestLogResetToSnapshot(t *testing.T) {
	l := NewLog()
	l.Append(Entry{Term: 1}, Entry{Term: 2}, Entry{Term: 2}, Entry{Term: 3})
	l.ResetToSnapshot(Snapshot{Index: 2, Term: 2})
	if l.LastIndex() != 4 {
		t.Fatalf("LastIndex=%d, want 4 (suffix kept)", l.LastIndex())
	}
	// 任期不一致时清空后缀。
	l2 := NewLog()
	l2.Append(Entry{Term: 1}, Entry{Term: 1}, Entry{Term: 5})
	l2.ResetToSnapshot(Snapshot{Index: 2, Term: 3})
	if l2.LastIndex() != 2 {
		t.Fatalf("LastIndex=%d, want 2 (suffix dropped)", l2.LastIndex())
	}
}
