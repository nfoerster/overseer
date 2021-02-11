package fetcher

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Minio struct {
	//Minio Host
	URL string
	//Access key falls back to env AWS_ACCESS_KEY, then metadata
	Access string
	//Secret key falls back to env AWS_SECRET_ACCESS_KEY, then metadata
	Secret string
	//Region defaults to ap-southeast-2
	Region string
	Bucket string
	Key    string
	UseSSL bool
	//Interval between checks
	Interval time.Duration
	//HeadTimeout defaults to 5 seconds
	HeadTimeout time.Duration
	//GetTimeout defaults to 5 minutes
	GetTimeout time.Duration
	//interal state
	client   *http.Client
	delay    bool
	lastETag string
}

// Init validates the provided config
func (s *Minio) Init() error {
	if s.Bucket == "" {
		return errors.New("S3 bucket not set")
	} else if s.Key == "" {
		return errors.New("S3 key not set")
	}
	if s.Region == "" {
		s.Region = "ap-southeast-2"
	}
	//initial etag
	if p, _ := os.Executable(); p != "" {
		if f, err := os.Open(p); err == nil {
			h := md5.New()
			io.Copy(h, f)
			f.Close()
			s.lastETag = hex.EncodeToString(h.Sum(nil))
		}
	}
	//apply defaults
	if s.Interval <= 0 {
		s.Interval = 5 * time.Minute
	}
	if s.HeadTimeout <= 0 {
		s.HeadTimeout = 5 * time.Second
	}
	if s.GetTimeout <= 0 {
		s.GetTimeout = 5 * time.Minute
	}
	return nil
}

// Fetch the binary from S3
func (s *Minio) Fetch() (io.Reader, error) {
	//delay fetches after first
	if s.delay {
		time.Sleep(s.Interval)
	}
	s.delay = true

	minioClient, err := minio.New(s.URL, &minio.Options{
		Creds:  credentials.NewStaticV4(s.Access, s.Secret, ""),
		Secure: s.UseSSL,
	})
	if err != nil {
		return nil, err
	}

	statInfo, err := minioClient.StatObject(context.Background(), s.Bucket, s.Key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	etag := statInfo.ETag
	if s.lastETag == etag {
		return nil, nil //skip, file match
	}

	obj, err := minioClient.GetObject(context.Background(), s.Bucket, s.Key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, statInfo.Size)
	read, err := obj.Read(buffer)
	if err != nil && err.Error() != "EOF" {
		return nil, err
	}
	if int64(read) != statInfo.Size {
		return nil, errors.New("Error during file receiving, stat size and size from getObject do not match")
	}
	s.lastETag = etag

	//success!
	return bytes.NewReader(buffer), nil
}
