package raft

import "log"

// Debugging
const Debug = false
const Eebug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func EPrintf(format string, a ...interface{}) (n int, err error) {
	if Eebug {
		log.Printf(format, a...)
	}
	return
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func min3(a int, b int, c int) int {
	return min(min(a, b), c)
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
