//go:build race

package raft

func init() { raceEnabled = true }
