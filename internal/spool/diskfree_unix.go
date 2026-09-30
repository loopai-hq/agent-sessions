//go:build unix

package spool

import "syscall"

// diskFree reports free and total bytes for the filesystem holding dir.
// syscall keeps this dependency-free; the client must install without a module
// cache or network, so every avoidable dependency is one more failure mode.
func diskFree(dir string) (free, total uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 0, err
	}
	// Bsize is int64 on Linux, uint32 on Darwin and uint64 on FreeBSD; Bavail
	// is uint64 on Linux and Darwin but int64 on FreeBSD. Both are positive
	// counts, and each conversion is what keeps the file building on the
	// GOOS where the field is signed or narrower. (.golangci.yml excludes
	// G115 and unconvert for this file for that reason.)
	bs := uint64(st.Bsize)
	return uint64(st.Bavail) * bs, st.Blocks * bs, nil
}
