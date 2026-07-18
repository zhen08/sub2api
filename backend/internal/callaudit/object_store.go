package callaudit

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type ObjectStoreConfig struct {
	Endpoint        string
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	Prefix          string
	ForcePathStyle  bool
}

type ObjectStore struct {
	client *s3.Client
	spool  *Spool
	cfg    ObjectStoreConfig
}

func NewObjectStore(ctx context.Context, spool *Spool, cfg ObjectStoreConfig) (*ObjectStore, error) {
	if spool == nil {
		return nil, fmt.Errorf("audit spool is required")
	}
	if strings.TrimSpace(cfg.Bucket) == "" || strings.TrimSpace(cfg.AccessKeyID) == "" || strings.TrimSpace(cfg.SecretAccessKey) == "" {
		return nil, fmt.Errorf("audit object storage bucket and credentials are required")
	}
	if strings.TrimSpace(cfg.Region) == "" {
		cfg.Region = "us-east-1"
	}
	if strings.TrimSpace(cfg.Prefix) == "" {
		cfg.Prefix = LegacyObjectPrefix
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load audit S3 config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(options *s3.Options) {
		if strings.TrimSpace(cfg.Endpoint) != "" {
			options.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		options.UsePathStyle = cfg.ForcePathStyle
		options.APIOptions = append(options.APIOptions, v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
		options.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
	return &ObjectStore{client: client, spool: spool, cfg: cfg}, nil
}

// Upload compresses through a 0600 temporary file. This keeps request/response
// artifacts off the heap even for long SSE calls while still producing the
// exact gzip-byte SHA-256 stored by the legacy service.
func (s *ObjectStore) Upload(ctx context.Context, manifest Manifest, artifact ArtifactRef) (StoredArtifact, error) {
	if s == nil || s.client == nil || s.spool == nil {
		return StoredArtifact{}, fmt.Errorf("audit object storage is not configured")
	}
	sourcePath, err := s.spool.safePath(artifact.SpoolPath)
	if err != nil {
		return StoredArtifact{}, err
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return StoredArtifact{}, fmt.Errorf("open audit artifact: %w", err)
	}
	defer source.Close()

	temp, err := os.CreateTemp(filepath.Dir(sourcePath), ".audit-upload-*.json.gz")
	if err != nil {
		return StoredArtifact{}, fmt.Errorf("create compressed audit artifact: %w", err)
	}
	tempPath := temp.Name()
	defer func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}()
	if err := temp.Chmod(0o600); err != nil {
		return StoredArtifact{}, fmt.Errorf("secure compressed audit artifact: %w", err)
	}
	hash := sha256.New()
	gzipWriter := gzip.NewWriter(io.MultiWriter(temp, hash))
	if _, err := io.Copy(gzipWriter, source); err != nil {
		_ = gzipWriter.Close()
		return StoredArtifact{}, fmt.Errorf("compress audit artifact: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return StoredArtifact{}, fmt.Errorf("finish audit compression: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return StoredArtifact{}, fmt.Errorf("sync compressed audit artifact: %w", err)
	}
	info, err := temp.Stat()
	if err != nil {
		return StoredArtifact{}, fmt.Errorf("stat compressed audit artifact: %w", err)
	}
	if _, err := temp.Seek(0, io.SeekStart); err != nil {
		return StoredArtifact{}, fmt.Errorf("rewind compressed audit artifact: %w", err)
	}
	objectKey, err := BuildLegacyObjectKey(s.cfg.Prefix, manifest.CreatedAt, manifest.APIKeyID, manifest.RequestID, artifact.Kind, artifact.Sequence)
	if err != nil {
		return StoredArtifact{}, err
	}
	digest := hash.Sum(nil)
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(s.cfg.Bucket),
		Key:               aws.String(objectKey),
		Body:              temp,
		ContentLength:     aws.Int64(info.Size()),
		ContentType:       aws.String("application/json"),
		ContentEncoding:   aws.String("gzip"),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumSHA256:    aws.String(base64.StdEncoding.EncodeToString(digest)),
		Metadata:          LegacyS3Metadata(manifest.RequestID, artifact.Kind),
	})
	if err != nil {
		return StoredArtifact{}, fmt.Errorf("upload audit artifact %s/%d: %w", artifact.Kind, artifact.Sequence, err)
	}
	return StoredArtifact{
		Kind:        artifact.Kind,
		Sequence:    artifact.Sequence,
		Bucket:      s.cfg.Bucket,
		ObjectKey:   objectKey,
		Bytes:       info.Size(),
		SHA256:      hex.EncodeToString(digest),
		ContentType: "application/json",
		CreatedAt:   time.Now().UTC(),
	}, nil
}

func (s *ObjectStore) Delete(ctx context.Context, bucket, objectKey string) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("audit object storage is not configured")
	}
	if strings.TrimSpace(bucket) == "" {
		bucket = s.cfg.Bucket
	}
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(objectKey)})
	return err
}
