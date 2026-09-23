package raft

import (
	"fmt"
	"os"
	"time"
)

// defaultLogger 返回带时间戳前缀的日志函数，供测试与 demo 使用。
func defaultLogger(id NodeID) func(format string, args ...any) {
	return func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "%s %s %s\n",
			time.Now().Format("15:04:05.000"), id, fmt.Sprintf(format, args...))
	}
}
