package raft

import "sync"

// HardState 是必须持久化的 Raft 状态。
type HardState struct {
	Term   Term
	VotedFor int // -1 表示未投票
	Commit Index
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

func (m *MemoryStorage) SaveHardState(hs HardState) error { return nil }
func (m *MemoryStorage) LoadHardState() (HardState, error) { return HardState{VotedFor: -1}, nil }
func (m *MemoryStorage) SaveEntries(entries []Entry) error { return nil }
func (m *MemoryStorage) LoadEntries() ([]Entry, error) { return nil, nil }
func (m *MemoryStorage) SaveSnapshot(s Snapshot) error { return nil }
func (m *MemoryStorage) LoadSnapshot() (Snapshot, error) { return Snapshot{}, nil }
