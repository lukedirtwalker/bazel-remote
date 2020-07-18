package nodisk

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"

	"github.com/buchgr/bazel-remote/cache"
	"github.com/buchgr/bazel-remote/config"
	"github.com/minio/minio-go/v6"
	"github.com/minio/minio-go/v6/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
)

const sha256HashStrSize = sha256.Size * 2 // Two hex characters per byte.
const emptySha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

var (
	cacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_s3_cache_hits",
		Help: "The total number of s3 backend cache hits",
	})
	cacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_s3_cache_misses",
		Help: "The total number of s3 backend cache misses",
	})
)

// Used in place of minio's verbose "NoSuchKey" error.
var errNotFound = errors.New("NOT FOUND")

type Cache struct {
	mcore        *minio.Core
	prefix       string
	bucket       string
	accessLogger cache.Logger
	errorLogger  cache.Logger
}

// New returns a new instance of the S3-API based cache
func New(s3Config *config.S3CloudStorageConfig, accessLogger cache.Logger,
	errorLogger cache.Logger) Cache {

	fmt.Println("Using S3 backend.")

	var minioCore *minio.Core
	var err error

	if s3Config.AccessKeyID != "" && s3Config.SecretAccessKey != "" {
		// Initialize minio client object.
		minioCore, err = minio.NewCore(
			s3Config.Endpoint,
			s3Config.AccessKeyID,
			s3Config.SecretAccessKey,
			!s3Config.DisableSSL,
		)

		if err != nil {
			log.Fatalln(err)
		}
	} else {
		// Initialize minio client object with IAM credentials
		creds := credentials.NewIAM(s3Config.IAMRoleEndpoint)
		minioClient, err := minio.NewWithCredentials(
			s3Config.Endpoint,
			creds,
			!s3Config.DisableSSL,
			s3Config.Region,
		)

		if err != nil {
			log.Fatalln(err)
		}

		minioCore = &minio.Core{
			Client: minioClient,
		}
	}

	c := Cache{
		mcore:        minioCore,
		prefix:       s3Config.Prefix,
		bucket:       s3Config.Bucket,
		accessLogger: accessLogger,
		errorLogger:  errorLogger,
	}
	return c
}

func (c *Cache) objectKey(hash string, kind cache.EntryKind) string {
	if c.prefix == "" {
		return fmt.Sprintf("%s/%s", kind, hash)
	}
	return fmt.Sprintf("%s/%s/%s", c.prefix, kind, hash)
}

func (c Cache) Put(kind cache.EntryKind, hash string, expectedSize int64, r io.Reader) error {

	// The hash format is checked properly in the http/grpc code.
	// Just perform a simple/fast check here, to catch bad tests.
	if len(hash) != sha256HashStrSize {
		return fmt.Errorf("Invalid hash size: %d, expected: %d",
			len(hash), sha256.Size)
	}

	if kind == cache.CAS && expectedSize == 0 && hash == emptySha256 {
		io.Copy(ioutil.Discard, r)
		return nil
	}

	uploadDigest := ""
	if kind == cache.CAS {
		uploadDigest = hash
	}

	key := c.objectKey(hash, kind)
	_, err := c.mcore.PutObject(
		c.bucket,     // bucketName
		key,          // objectName
		r,            // reader
		expectedSize, // objectSize
		"",           // md5base64
		uploadDigest, // sha256
		minio.PutObjectOptions{
			UserMetadata: map[string]string{
				"Content-Type": "application/octet-stream",
			},
		}, // metadata
	)
	logResponse(c.accessLogger, "UPLOAD", c.bucket, key, err)
	return err
}

func (c Cache) Get(kind cache.EntryKind, hash string, size int64) (io.ReadCloser, int64, error) {
	key := c.objectKey(hash, kind)
	object, info, _, err := c.mcore.GetObject(
		c.bucket,                 // bucketName
		key,                      // objectName
		minio.GetObjectOptions{}, // opts
	)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			cacheMisses.Inc()
			logResponse(c.accessLogger, "DOWNLOAD", c.bucket, key, errNotFound)
			return nil, -1, nil
		}
		cacheMisses.Inc()
		logResponse(c.accessLogger, "DOWNLOAD", c.bucket, key, err)
		return nil, -1, err
	}
	cacheHits.Inc()

	logResponse(c.accessLogger, "DOWNLOAD", c.bucket, key, nil)

	return object, info.Size, nil
}

func (c Cache) Contains(kind cache.EntryKind, hash string, size int64) (bool, int64) {
	foundSize := int64(-1)

	s, err := c.mcore.StatObject(
		c.bucket,                  // bucketName
		c.objectKey(hash, kind),   // objectName
		minio.StatObjectOptions{}, // opts
	)

	exists := (err == nil)
	if err != nil {
		err = errNotFound
	} else {
		foundSize = s.Size
	}

	logResponse(c.accessLogger, "CONTAINS", c.bucket, c.objectKey(hash, kind), err)
	return exists, foundSize
}

// MaxSize returns the maximum cache size in bytes.
func (c Cache) MaxSize() int64 {
	// The underlying value is never modified, no need to lock.
	return -1
}

// Stats returns the current size of the cache in bytes, and the number of
// items stored in the cache.
func (c Cache) Stats() (currentSize int64, numItems int) {
	return -1, -1
}

func (c Cache) GetValidatedActionResult(hash string) (*pb.ActionResult, []byte, error) {

	rdr, sizeBytes, err := c.Get(cache.AC, hash, -1)
	if err != nil {
		return nil, nil, err
	}

	if rdr == nil || sizeBytes <= 0 {
		return nil, nil, nil // aka "not found"
	}

	acdata, err := ioutil.ReadAll(rdr)
	if err != nil {
		return nil, nil, err
	}

	result := &pb.ActionResult{}
	err = proto.Unmarshal(acdata, result)
	if err != nil {
		return nil, nil, err
	}

	for _, f := range result.OutputFiles {
		if len(f.Contents) == 0 {
			found, _ := c.Contains(cache.CAS, f.Digest.Hash, f.Digest.SizeBytes)
			if !found {
				return nil, nil, nil // aka "not found"
			}
		}
	}

	for _, d := range result.OutputDirectories {
		r, size, err := c.Get(cache.CAS, d.TreeDigest.Hash, d.TreeDigest.SizeBytes)
		if r == nil {
			return nil, nil, err // aka "not found", or an err if non-nil
		}
		if err != nil {
			r.Close()
			return nil, nil, err
		}
		if size != d.TreeDigest.SizeBytes {
			r.Close()
			return nil, nil, fmt.Errorf("expected %d bytes, found %d",
				d.TreeDigest.SizeBytes, size)
		}

		var oddata []byte
		oddata, err = ioutil.ReadAll(r)
		r.Close()
		if err != nil {
			return nil, nil, err
		}

		tree := pb.Tree{}
		err = proto.Unmarshal(oddata, &tree)
		if err != nil {
			return nil, nil, err
		}

		for _, f := range tree.Root.GetFiles() {
			if f.Digest == nil {
				continue
			}
			found, _ := c.Contains(cache.CAS, f.Digest.Hash, f.Digest.SizeBytes)
			if !found {
				return nil, nil, nil // aka "not found"
			}
		}

		for _, child := range tree.GetChildren() {
			for _, f := range child.GetFiles() {
				if f.Digest == nil {
					continue
				}
				found, _ := c.Contains(cache.CAS, f.Digest.Hash, f.Digest.SizeBytes)
				if !found {
					return nil, nil, nil // aka "not found"
				}
			}
		}
	}

	if result.StdoutDigest != nil {
		found, _ := c.Contains(cache.CAS, result.StdoutDigest.Hash, result.StdoutDigest.SizeBytes)
		if !found {
			return nil, nil, nil // aka "not found"
		}
	}

	if result.StderrDigest != nil {
		found, _ := c.Contains(cache.CAS, result.StderrDigest.Hash, result.StderrDigest.SizeBytes)
		if !found {
			return nil, nil, nil // aka "not found"
		}
	}

	return result, acdata, nil
}

// Helper function for logging responses
func logResponse(log cache.Logger, method, bucket, key string, err error) {
	status := "OK"
	if err != nil {
		status = err.Error()
	}

	log.Printf("S3 %s %s %s %s", method, bucket, key, status)
}
