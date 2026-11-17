package graph

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// The signer is checked against two signatures published by AWS, so it is
// not only tested against code written alongside it.
func TestSignV4KnownVectors(t *testing.T) {
	// AWS SigV4 test suite, "get-vanilla".
	got := signV4("GET", "/", "", map[string]string{
		"host":       "example.amazonaws.com",
		"x-amz-date": "20150830T123600Z",
	}, emptySHA256, "AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "us-east-1", "service", "20150830T123600Z")
	want := "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, " +
		"SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
	if got != want {
		t.Fatalf("get-vanilla:\n got %s\nwant %s", got, want)
	}

	// Amazon S3 API reference, "GET Object" example.
	got = signV4("GET", "/test.txt", "", map[string]string{
		"host":                 "examplebucket.s3.amazonaws.com",
		"range":                "bytes=0-9",
		"x-amz-content-sha256": emptySHA256,
		"x-amz-date":           "20130524T000000Z",
	}, emptySHA256, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1", "s3", "20130524T000000Z")
	want = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, " +
		"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if got != want {
		t.Fatalf("s3 get object:\n got %s\nwant %s", got, want)
	}
}

// fakeS3 is an in-memory stand-in for the two S3 calls the store makes. It
// does not verify signatures (the vectors above and a real MinIO run cover
// that); it checks the requests are shaped correctly.
func fakeS3(t *testing.T) (*httptest.Server, *sync.Map) {
	var objects sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=key/") ||
			r.Header.Get("X-Amz-Date") == "" || len(r.Header.Get("X-Amz-Content-Sha256")) != 64 {
			t.Errorf("request is not SigV4 signed: %v", r.Header)
		}
		if !strings.HasPrefix(r.URL.Path, "/graphs/") {
			t.Errorf("expected path-style URL under /graphs/, got %s", r.URL.Path)
		}
		switch r.Method {
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			objects.Store(r.URL.Path, body)
		case http.MethodGet:
			body, ok := objects.Load(r.URL.Path)
			if !ok {
				http.Error(w, "<Error><Code>NoSuchKey</Code></Error>", http.StatusNotFound)
				return
			}
			w.Write(body.([]byte))
		default:
			http.Error(w, "unsupported", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &objects
}

func TestS3StoreRoundTrip(t *testing.T) {
	srv, _ := fakeS3(t)
	s3 := S3Store{Endpoint: srv.URL, Bucket: "graphs", Region: "us-east-1", AccessKey: "key", SecretKey: "secret"}
	if _, err := s3.Get("missing.json"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing object should be os.ErrNotExist, got %v", err)
	}
	if err := s3.Put("a/b.json", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err := s3.Get("a/b.json")
	if err != nil || string(got) != "hello" {
		t.Fatalf("got %q, %v", got, err)
	}
}

// A machine that lost its disk recovers the last checkpoint from the object
// store alone.
func TestCheckpointToObjectStoreAndRecoverOnEmptyDisk(t *testing.T) {
	srv, objects := fakeS3(t)
	s3 := S3Store{Endpoint: srv.URL, Bucket: "graphs", Region: "us-east-1", AccessKey: "key", SecretKey: "secret"}

	s, err := OpenWith(t.TempDir(), false, s3)
	if err != nil {
		t.Fatal(err)
	}
	a, b := s.AllocID(), s.AllocID()
	err = s.Commit([]Op{
		{Kind: "node", ID: a, Label: "P", Props: map[string]any{"name": "Alice"}},
		{Kind: "node", ID: b, Label: "P", Props: map[string]any{"name": "Bob"}},
		{Kind: "edge", ID: s.AllocID(), Label: "KNOWS", From: a, To: b},
		{Kind: "index", Label: "P", Key: "name"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if _, ok := objects.Load("/graphs/snapshot.json"); !ok {
		t.Fatal("snapshot was not uploaded")
	}

	s2, err := OpenWith(t.TempDir(), false, s3) // different, empty directory
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	sn := s2.Snapshot()
	ids, ok := sn.Lookup("P", "name", "Alice")
	if !ok || len(ids) != 1 || ids[0] != a {
		t.Fatalf("index or node not recovered: %v %v", ids, ok)
	}
	if nb := sn.Neighbors(a, true, "KNOWS", nil); len(nb) != 1 || nb[0] != b {
		t.Fatalf("edge not recovered: %v", nb)
	}
	if id := s2.AllocID(); id <= b {
		t.Fatalf("id counter not recovered, got %d", id)
	}
}
