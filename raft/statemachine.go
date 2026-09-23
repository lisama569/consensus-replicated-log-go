package raft

import (
	"encoding/json"
	"sync"
)

// KVStateMachine 是一个极简键值状态机。
//
// 并发约定：
//   - Apply 仅由节点事件循环串行调用；
//   - Snapshot 也在事件循环内构造（与 Apply 天然互斥）；
//   - Restore 仅在启动恢复阶段、事件循环启动前调用；
//   - Lookup 供客户端线性一致读使用（领导权确认后），可能与 Apply 并发，
//     因此用 RWMutex 保护；Snapshot 用写锁、Apply 用写锁、Lookup 用读锁。
type KVStateMachine struct {
	mu sync.RWMutex
	kv map[string]string
	// lastSeq 记录每个客户端已应用的最大序列号，用于幂等去重。
	lastSeq map[string]uint64
}

// snapshotData 是状态机快照的序列化内容。
type snapshotData struct {
	KV      map[string]string `json:"kv"`
	LastSeq map[string]uint64 `json:"last_seq"`
}

func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{
		kv:      make(map[string]string),
		lastSeq: make(map[string]uint64),
	}
}

// Apply 应用一条命令，返回 true 表示首次应用，false 表示重复请求被跳过。
func (sm *KVStateMachine) Apply(cmd Command) (bool, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 幂等去重：同一 ClientID 下，仅当 Seq 严格大于已应用 Seq 时才执行。
	if cmd.ClientID != "" {
		if last, ok := sm.lastSeq[cmd.ClientID]; ok && cmd.Seq <= last {
			return false, nil
		}
	}

	switch cmd.Op {
	case OpSet:
		sm.kv[cmd.Key] = cmd.Value
	case OpNoop:
		// 领导者上任后的空操作，仅用于推动提交位置。
	default:
		// 未知操作不改变状态机（协议仍记录该日志，保证各节点日志一致）。
	}

	if cmd.ClientID != "" {
		sm.lastSeq[cmd.ClientID] = cmd.Seq
	}
	return true, nil
}

// Lookup 读取键值。
func (sm *KVStateMachine) Lookup(key string) (string, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	v, ok := sm.kv[key]
	return v, ok
}

// Snapshot 导出状态机字节（写锁，保证与并发 Lookup 之外的任何修改互斥）。
func (sm *KVStateMachine) Snapshot() ([]byte, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	data := snapshotData{
		KV:      make(map[string]string, len(sm.kv)),
		LastSeq: make(map[string]uint64, len(sm.lastSeq)),
	}
	for k, v := range sm.kv {
		data.KV[k] = v
	}
	for k, v := range sm.lastSeq {
		data.LastSeq[k] = v
	}
	return json.Marshal(&data)
}

// Restore 从字节恢复。
func (sm *KVStateMachine) Restore(data []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var d snapshotData
	if len(data) == 0 {
		d = snapshotData{KV: map[string]string{}, LastSeq: map[string]uint64{}}
	} else if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	if d.KV == nil {
		d.KV = map[string]string{}
	}
	if d.LastSeq == nil {
		d.LastSeq = map[string]uint64{}
	}
	sm.kv = d.KV
	sm.lastSeq = d.LastSeq
	return nil
}
