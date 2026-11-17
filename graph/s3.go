package graph

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// S3Store is an ObjectStore backed by any S3-compatible service (MinIO,
// AWS S3). It uses path-style URLs (endpoint/bucket/key), which is what MinIO
// expects, and signs requests with AWS Signature Version 4. The bucket must
// already exist.
type S3Store struct {
	Endpoint  string // e.g. http://localhost:9000
	Bucket    string
	Region    string // MinIO accepts us-east-1 by default
	AccessKey string
	SecretKey string
	Client    *http.Client // nil means a client with a 30s timeout
}

func (s S3Store) Put(key string, data []byte) error {
	resp, err := s.do(http.MethodPut, key, data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return s3Error("put", key, resp)
	}
	return nil
}

// Get returns an error wrapping os.ErrNotExist when the object is missing,
// the same contract as FSStore, so recovery treats it as "no snapshot yet".
func (s S3Store) Get(key string) ([]byte, error) {
	resp, err := s.do(http.MethodGet, key, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		if strings.Contains(string(body), "NoSuchBucket") {
			return nil, fmt.Errorf("s3 get %s: bucket %q does not exist", key, s.Bucket)
		}
		return nil, fmt.Errorf("s3 get %s: %w", key, os.ErrNotExist)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, s3Error("get", key, resp)
	}
	return io.ReadAll(resp.Body)
}

func s3Error(op, key string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return fmt.Errorf("s3 %s %s: %s: %s", op, key, resp.Status, strings.TrimSpace(string(body)))
}

func (s S3Store) do(method, key string, body []byte) (*http.Response, error) {
	base, err := url.Parse(s.Endpoint)
	if err != nil || base.Host == "" {
		return nil, fmt.Errorf("invalid s3 endpoint %q", s.Endpoint)
	}
	canonicalURI := "/" + uriEncode(s.Bucket, true) + "/" + uriEncode(key, false)
	req, err := http.NewRequest(method, base.Scheme+"://"+base.Host+canonicalURI, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])
	amzDate := time.Now().UTC().Format("20060102T150405Z")
	headers := map[string]string{
		"host":                 base.Host,
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           amzDate,
	}
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("Authorization", signV4(method, canonicalURI, "", headers, payloadHash, s.AccessKey, s.SecretKey, s.Region, "s3", amzDate))
	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return client.Do(req)
}

// signV4 builds the Authorization header for AWS Signature Version 4.
// headers must use lowercase names; every header passed in is signed.
func signV4(method, canonicalURI, canonicalQuery string, headers map[string]string, payloadHash, accessKey, secretKey, region, service, amzDate string) string {
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	var canonicalHeaders strings.Builder
	for _, name := range names {
		canonicalHeaders.WriteString(name + ":" + strings.TrimSpace(headers[name]) + "\n")
	}
	signedHeaders := strings.Join(names, ";")
	canonicalRequest := strings.Join([]string{
		method, canonicalURI, canonicalQuery, canonicalHeaders.String(), signedHeaders, payloadHash,
	}, "\n")

	date := amzDate[:8]
	scope := date + "/" + region + "/" + service + "/aws4_request"
	requestHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(requestHash[:])

	key := hmacSHA256([]byte("AWS4"+secretKey), date)
	key = hmacSHA256(key, region)
	key = hmacSHA256(key, service)
	key = hmacSHA256(key, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(key, stringToSign))

	return "AWS4-HMAC-SHA256 Credential=" + accessKey + "/" + scope +
		", SignedHeaders=" + signedHeaders + ", Signature=" + signature
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// uriEncode percent-encodes everything except the RFC 3986 unreserved
// characters, as SigV4 requires. Slashes in object keys are kept.
func uriEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		unreserved := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~'
		if unreserved || (c == '/' && !encodeSlash) {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}
