package server

import (
	"io"

	"github.com/buchgr/bazel-remote/cache"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// Cache is the interface a cache has to implement.
type Cache interface {
	Get(kind cache.EntryKind, hash string, size int64) (io.ReadCloser, int64, error)
	Contains(kind cache.EntryKind, hash string, size int64) (bool, int64)
	Put(kind cache.EntryKind, hash string, expectedSize int64, r io.Reader) error
	GetValidatedActionResult(hash string) (*pb.ActionResult, []byte, error)

	MaxSize() int64
	Stats() (currentSize int64, numItems int)
}
