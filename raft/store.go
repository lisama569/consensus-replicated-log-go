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
	// 分块快照接收中的检查点与部分数据（可恢复续传）。
	incomingMetaFile = "snapshot.incoming.json"
	incomingDataFile = "snapshot.incoming"
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

	// 分块接收（内存模式）与数据文件句柄（文件模式）。
	memIncoming     *persistedIncoming
	memIncomingData []byte
	incFile         *os.File
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

// Close 刷新并关闭（当前实现每次写入即 fsync，这里关闭部分数据文件句柄）。
func (s *FileStore) Close() error {
	if s.incFile != nil {
		err := s.incFile.Close()
		s.incFile = nil
		return err
	}
	return nil
}

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

// ---- 分块快照接收（可恢复续传） ----

// persistedIncoming 是分块接收会话的检查点（崩溃后可从此续传）。
type persistedIncoming struct {
	SessionID         string `json:"session_id"`
	LastIncludedIndex Index  `json:"last_included_index"`
	LastIncludedTerm  Term   `json:"last_included_term"`
	Term              Term   `json:"term"`
	Total             uint64 `json:"total"`
	Received          uint64 `json:"received"`
	CRC               uint32 `json:"crc"`
}

// SaveIncomingCheckpoint 原子持久化接收检查点（先写数据、后更新检查点，
// 崩溃时数据文件可能多于检查点，加载时按检查点截断，绝不少收）。
func (s *FileStore) SaveIncomingCheckpoint(p persistedIncoming) error {
	if s.dir == "" {
		cp := p
		s.memIncoming = &cp
		return nil
	}
	return writeJSONAtomic(filepath.Join(s.dir, incomingMetaFile), &p)
}

// AppendIncomingChunk 把一块数据追加到部分快照文件的 offset 处并 fsync。
// 接收端保证只追加连续前缀（offset == 已确认长度），重复前缀不会走到这里。
func (s *FileStore) AppendIncomingChunk(offset uint64, data []byte) error {
	if s.dir == "" {
		if offset != uint64(len(s.memIncomingData)) {
			return errors.New("raft: incoming chunk offset mismatch")
		}
		s.memIncomingData = append(s.memIncomingData, data...)
		return nil
	}
	f, err := s.incomingFile()
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(data, int64(offset)); err != nil {
		return err
	}
	// 截断到写入末尾：防御性清理可能残留的旧会话尾部字节。
	if err := f.Truncate(int64(offset) + int64(len(data))); err != nil {
		return err
	}
	return f.Sync()
}

// incomingFile 懒打开部分数据文件句柄（避免每块开关文件）。
func (s *FileStore) incomingFile() (*os.File, error) {
	if s.incFile != nil {
		return s.incFile, nil
	}
	f, err := os.OpenFile(filepath.Join(s.dir, incomingDataFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	s.incFile = f
	return f, nil
}

// LoadIncoming 读取接收检查点与已确认的部分数据；无在途会话时返回 nil。
// 数据文件可能比检查点长（写完数据、未写检查点时崩溃），按检查点长度截断。
func (s *FileStore) LoadIncoming() (*persistedIncoming, []byte, error) {
	if s.dir == "" {
		if s.memIncoming == nil {
			return nil, nil, nil
		}
		cp := *s.memIncoming
		data := append([]byte(nil), s.memIncomingData...)
		if uint64(len(data)) > cp.Received {
			data = data[:cp.Received]
		}
		return &cp, data, nil
	}
	var p persistedIncoming
	ok, err := readJSON(filepath.Join(s.dir, incomingMetaFile), &p)
	if err != nil || !ok {
		return nil, nil, err
	}
	raw, err := os.ReadFile(filepath.Join(s.dir, incomingDataFile))
	if errors.Is(err, os.ErrNotExist) {
		raw = nil
	} else if err != nil {
		return nil, nil, err
	}
	if uint64(len(raw)) > p.Received {
		raw = raw[:p.Received]
	}
	return &p, raw, nil
}

// ClearIncoming 删除在途会话的检查点与部分数据（安装完成/取消/被取代时调用）。
func (s *FileStore) ClearIncoming() error {
	if s.dir == "" {
		s.memIncoming = nil
		s.memIncomingData = nil
		return nil
	}
	if s.incFile != nil {
		_ = s.incFile.Close()
		s.incFile = nil
	}
	for _, f := range []string{incomingMetaFile, incomingDataFile} {
		if err := os.Remove(filepath.Join(s.dir, f)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
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
