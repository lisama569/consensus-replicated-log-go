package raft

import (
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
)

// 分块快照在接收端的临时落盘（staging）：
//   - snapshot.staging.json：本次传输的固定元数据（末索引/任期/总长/校验和）；
//   - snapshot.staging.data：已连续接收的原始快照字节。
//
// 只有整份快照收齐、校验和通过后才会被节点逻辑提升为正式快照；
// 崩溃/取消/失败时半份数据永远不会被当成可用状态。

const (
	stagingMetaFile = "snapshot.staging.json"
	stagingDataFile = "snapshot.staging.data"
	stagingVersion  = 1
)

// StagingMeta 是进行中的分块快照传输元数据。
// 同一会话的身份由 (LastIncludedIndex, LastIncludedTerm, Length, Checksum) 唯一确定；
// TransferID 仅用于日志关联与用户侧进度展示。
type StagingMeta struct {
	TransferID        uint64
	LastIncludedIndex Index
	LastIncludedTerm  Term
	Length            uint64
	Checksum          []byte
	Leader            NodeID
}

// persistedStaging 是临时快照元数据的磁盘格式。
type persistedStaging struct {
	Version           int    `json:"version"`
	TransferID        uint64 `json:"transfer_id"`
	LastIncludedIndex Index  `json:"last_included_index"`
	LastIncludedTerm  Term   `json:"last_included_term"`
	Length            uint64 `json:"length"`
	Checksum          string `json:"checksum"`
	Leader            NodeID `json:"leader"`
}

func (s *FileStore) stagingPath() string { return filepath.Join(s.dir, stagingMetaFile) }
func (s *FileStore) stagingDataPath() string {
	return filepath.Join(s.dir, stagingDataFile)
}

// ---- 临时快照：元数据 ----

func (s *FileStore) SaveStagingMeta(m StagingMeta) error {
	p := persistedStaging{
		Version:           stagingVersion,
		TransferID:        m.TransferID,
		LastIncludedIndex: m.LastIncludedIndex,
		LastIncludedTerm:  m.LastIncludedTerm,
		Length:            m.Length,
		Checksum:          base64.StdEncoding.EncodeToString(m.Checksum),
		Leader:            m.Leader,
	}
	if s.dir == "" {
		cp := p
		s.memStaging = &cp
		return nil
	}
	return writeJSONAtomic(s.stagingPath(), &p)
}

func (s *FileStore) LoadStagingMeta() (StagingMeta, bool, error) {
	var p persistedStaging
	if s.dir == "" {
		if s.memStaging == nil {
			return StagingMeta{}, false, nil
		}
		p = *s.memStaging
	} else {
		ok, err := readJSON(s.stagingPath(), &p)
		if err != nil {
			return StagingMeta{}, false, err
		}
		if !ok {
			return StagingMeta{}, false, nil
		}
	}
	// 旧/损坏版本的临时数据：忽略并清理，绝不使用。
	if p.Version != stagingVersion || p.LastIncludedIndex == 0 || p.Length == 0 {
		_ = s.ClearStaging()
		return StagingMeta{}, false, nil
	}
	sum, err := base64.StdEncoding.DecodeString(p.Checksum)
	if err != nil || len(sum) == 0 {
		_ = s.ClearStaging()
		return StagingMeta{}, false, nil
	}
	return StagingMeta{
		TransferID:        p.TransferID,
		LastIncludedIndex: p.LastIncludedIndex,
		LastIncludedTerm:  p.LastIncludedTerm,
		Length:            p.Length,
		Checksum:          sum,
		Leader:            p.Leader,
	}, true, nil
}

func (s *FileStore) ClearStaging() error {
	if s.dir == "" {
		s.memStaging = nil
		s.memStagingData = nil
		return nil
	}
	_ = os.Remove(s.stagingDataPath())
	return os.Remove(s.stagingPath())
}

// ---- 临时快照：数据分块 ----

// StartStagingData 截断并重建数据文件（开始一份全新的临时快照）。
func (s *FileStore) StartStagingData() error {
	if s.dir == "" {
		s.memStagingData = []byte{}
		return nil
	}
	return writeFileAtomic(s.stagingDataPath(), nil)
}

// AppendStagingChunk 在 offset 处写入一块数据；offset 必须严格等于当前文件长度。
// 写入采用 O_APPEND + fsync，返回前该块已 durable，崩溃重启后仍可从断点续传。
func (s *FileStore) AppendStagingChunk(offset uint64, data []byte) error {
	if s.dir == "" {
		if uint64(len(s.memStagingData)) != offset {
			return errors.New("raft: staging chunk offset mismatch")
		}
		// 深拷贝：data 可能是某个大缓冲的切片（如分块切自同一份快照），
		// append 复用底层数组会让后写的块覆盖先前块的内存。
		cp := make([]byte, len(data))
		copy(cp, data)
		s.memStagingData = append(s.memStagingData, cp...)
		return nil
	}
	f, err := os.OpenFile(s.stagingDataPath(), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err == nil && uint64(info.Size()) != offset {
		f.Close()
		return errors.New("raft: staging chunk offset mismatch")
	}
	if _, werr := f.Write(data); werr != nil {
		f.Close()
		return werr
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// StagingSize 返回临时数据文件当前长度。
func (s *FileStore) StagingSize() (uint64, error) {
	if s.dir == "" {
		return uint64(len(s.memStagingData)), nil
	}
	info, err := os.Stat(s.stagingDataPath())
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return uint64(info.Size()), nil
}

// LoadStagingData 读取临时快照全部已收字节（收齐后做整体校验与安装）。
func (s *FileStore) LoadStagingData() ([]byte, error) {
	if s.dir == "" {
		return append([]byte(nil), s.memStagingData...), nil
	}
	return os.ReadFile(s.stagingDataPath())
}

// writeFileAtomic 与 writeJSONAtomic 同样的 tmp+fsync+rename 语义，用于二进制数据文件。
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
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
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
