package raft

import "sync"

// HardState 是必须持久化的 Raft 状态。
// 不变式：Term 单调不减；Commit 单调不减；同一 Term 内 VotedFor 不变。
type HardState struct {
	Term     Term
	VotedFor int // -1 表示未投票
	Commit   Index
}

// Storage 是持久化接口：重启后已提交内容与投票记录不得丢失。
type Storage interface {
	SaveHardState(hs HardState) error
	LoadHardState() (HardState, error)
	SaveEntries(entries []Entry) error
	LoadEntries() ([]Entry, error)
	SaveSnapshot(s Snapshot) error
	LoadSnapshot() (Snapshot, error)
}

// MemoryStorage 模拟持久存储：进程内保存，节点崩溃重启后数据仍在。
// 通过深拷贝隔离读写，避免并发访问时的数据竞争。
type MemoryStorage struct {
	mu      sync.Mutex
	hs      HardState
	entries []Entry
	snap    Snapshot
	hasSnap bool
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{hs: HardState{VotedFor: -1}}
}

func (m *MemoryStorage) SaveHardState(hs HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs = hs
	return nil
}

func (m *MemoryStorage) LoadHardState() (HardState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hs, nil
}

func (m *MemoryStorage) SaveEntries(entries []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append([]Entry(nil), entries...)
	return nil
}

func (m *MemoryStorage) LoadEntries() ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Entry(nil), m.entries...), nil
}

func (m *MemoryStorage) SaveSnapshot(s Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snap = Snapshot{Index: s.Index, Term: s.Term, Data: append([]byte(nil), s.Data...)}
	m.hasSnap = true
	return nil
}

func (m *MemoryStorage) LoadSnapshot() (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasSnap {
		return Snapshot{}, nil
	}
	s := m.snap
	s.Data = append([]byte(nil), m.snap.Data...)
	return s, nil
}
