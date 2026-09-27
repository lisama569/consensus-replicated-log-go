package raft

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

const (
	stateFile    = "state.json"
	logFile      = "log.json"
	snapshotFile = "snapshot.json"
)

// SnapshotMeta 描述快照覆盖范围。
type SnapshotMeta struct {
	LastIncludedIndex Index
	LastIncludedTerm  Term
}

// persistedState 是硬状态（currentTerm/votedFor）的磁盘格式。
type persistedState struct {
	Term     Term   `json:"term"`
	VotedFor NodeID `json:"voted_for"`
}

// persistedSnapshot 是快照文件格式：元数据 + 状态机字节（base64）。
type persistedSnapshot struct {
	LastIncludedIndex Index  `json:"last_included_index"`
	LastIncludedTerm  Term   `json:"last_included_term"`
	Data              string `json:"data"`
}

// FileStore 负责节点的持久化：当前任期、投票、日志后缀、快照。
// 所有方法均由节点事件循环串行调用；Open/Close 可在循环外调用。
// dir == "" 时退化为纯内存实现（不支持崩溃重启，仅供不需要重启的测试使用）。
type FileStore struct {
	dir string

	// 内存模式（dir == ""）下使用的字段。
	memTerm  Term
	memVoted NodeID
	memLog   []LogEntry
	memSnap  *persistedSnapshot
	hasState bool
	hasSnap  bool

	// 内存模式下的临时分块快照。
	memStaging     *persistedStaging
	memStagingData []byte
}

// OpenStore 打开（不存在则初始化）数据目录。
func OpenStore(dir string) (*FileStore, error) {
	s := &FileStore{dir: dir}
	if dir == "" {
		return s, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return s, nil
}

// Close 刷新并关闭（当前实现每次写入即 fsync，这里仅作占位）。
func (s *FileStore) Close() error { return nil }

// ---- 硬状态 ----

func (s *FileStore) LoadState() (term Term, votedFor NodeID, err error) {
	if s.dir == "" {
		if !s.hasState {
			return 0, "", nil
		}
		return s.memTerm, s.memVoted, nil
	}
	var p persistedState
	ok, err := readJSON(filepath.Join(s.dir, stateFile), &p)
	if err != nil || !ok {
		return 0, "", err
	}
	return p.Term, p.VotedFor, nil
}

func (s *FileStore) SaveState(term Term, votedFor NodeID) error {
	if s.dir == "" {
		s.memTerm, s.memVoted, s.hasState = term, votedFor, true
		return nil
	}
	return writeJSONAtomic(filepath.Join(s.dir, stateFile), &persistedState{Term: term, VotedFor: votedFor})
}

// ---- 快照 ----

func (s *FileStore) SaveSnapshot(meta SnapshotMeta, data []byte) error {
	p := persistedSnapshot{
		LastIncludedIndex: meta.LastIncludedIndex,
		LastIncludedTerm:  meta.LastIncludedTerm,
		Data:              base64.StdEncoding.EncodeToString(data),
	}
	if s.dir == "" {
		cp := p
		s.memSnap, s.hasSnap = &cp, true
		return nil
	}
	return writeJSONAtomic(filepath.Join(s.dir, snapshotFile), &p)
}

func (s *FileStore) LoadSnapshot() (meta SnapshotMeta, data []byte, err error) {
	var p persistedSnapshot
	if s.dir == "" {
		if !s.hasSnap {
			return SnapshotMeta{}, nil, nil
		}
		p = *s.memSnap
	} else {
		ok, rerr := readJSON(filepath.Join(s.dir, snapshotFile), &p)
		if rerr != nil {
			return SnapshotMeta{}, nil, rerr
		}
		if !ok {
			return SnapshotMeta{}, nil, nil
		}
	}
	decoded, err := base64.StdEncoding.DecodeString(p.Data)
	if err != nil {
		return SnapshotMeta{}, nil, err
	}
	return SnapshotMeta{LastIncludedIndex: p.LastIncludedIndex, LastIncludedTerm: p.LastIncludedTerm}, decoded, nil
}

func (s *FileStore) HasSnapshot() bool {
	if s.dir == "" {
		return s.hasSnap
	}
	_, err := os.Stat(filepath.Join(s.dir, snapshotFile))
	return err == nil
}

// ---- 日志 ----

// SaveLogSuffix 用 entries 整体替换日志文件。
// entries 必须是快照之后的全部日志（索引严格大于快照最后索引）。
func (s *FileStore) SaveLogSuffix(entries []LogEntry) error {
	if s.dir == "" {
		s.memLog = append([]LogEntry(nil), entries...)
		return nil
	}
	return writeJSONAtomic(filepath.Join(s.dir, logFile), &entries)
}

// LoadLogEntries 返回快照之后的日志后缀；
// 自动丢弃任何索引 <= 快照最后索引的残留条目（快照与日志压缩之间崩溃的情形）。
func (s *FileStore) LoadLogEntries() ([]LogEntry, error) {
	var entries []LogEntry
	if s.dir == "" {
		entries = append(entries, s.memLog...)
	} else {
		ok, err := readJSON(filepath.Join(s.dir, logFile), &entries)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
	}
	var cut Index
	if s.HasSnapshot() {
		meta, _, err := s.LoadSnapshot()
		if err != nil {
			return nil, err
		}
		cut = meta.LastIncludedIndex
	}
	out := entries[:0:0]
	for _, e := range entries {
		if e.Index > cut {
			out = append(out, e)
		}
	}
	return out, nil
}

// ---- 文件辅助 ----

func readJSON(path string, v any) (bool, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(b) == 0 {
		return false, nil
	}
	if err := json.Unmarshal(b, v); err != nil {
		return false, err
	}
	return true, nil
}

// writeJSONAtomic 先写临时文件、fsync，再原子 rename，保证崩溃时不会出现半写文件。
func writeJSONAtomic(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	// fsync 目录，保证 rename 落盘。
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
