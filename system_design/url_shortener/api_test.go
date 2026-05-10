package url_shortener

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/handlers"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/server"
)

func TestAPI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "API tests")
}

const DisableLogsLevel slog.Level = 20

var _ = Describe("Interacting with URL shortener API", func() {
	var srv *http.Server
	var recorder *httptest.ResponseRecorder
	var hasher *FakeHasher
	var store *FakeDataStore
	var cache *FakeCacheStore
	var logger *slog.Logger

	BeforeEach(func() {
		recorder = httptest.NewRecorder()
		store = &FakeDataStore{
			Data:   make(map[string]*handlers.ShortenedRecord),
			Visits: make(map[int]*handlers.VisitRecord),
		}
		hasher = &FakeHasher{}
		cache = &FakeCacheStore{Cache: make(map[string]string)}
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: DisableLogsLevel,
		}))

		srv = server.NewServer(logger, hasher, store, cache)
	})
	Context("creating a shortened link", func() {
		When("given valid parameters", func() {
			It("returns a 200", func() {
				request, _ := newShortenedRequest("blah")

				srv.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))

			})
			It("returns the url", func() {
				request, _ := newShortenedRequest("blah")

				srv.Handler.ServeHTTP(recorder, request)

				resp := decodeTestResponse[handlers.ShortenedResponse](recorder.Body)
				Expect(resp.URL).To(Equal("blah+hello"))
			})
			It("stores the value in the database", func() {
				request, data := newShortenedRequest("blah")

				srv.Handler.ServeHTTP(recorder, request)

				encoded := hasher.Encode(data.URL)
				Expect(store.Data[encoded].TargetURL).To(Equal(data.URL))
			})
			It("caches the value", func() {
				request, data := newShortenedRequest("blah")

				srv.Handler.ServeHTTP(recorder, request)

				encoded := hasher.Encode(data.URL)
				Expect(cache.Cache[data.URL]).To(Equal(encoded))
			})
		})
		When("given a blank string", func() {
			It("will return 400", func() {
				request, _ := newShortenedRequest("")

				srv.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusBadRequest))
			})
			It("will tell you invalid parameters", func() {
				request, _ := newShortenedRequest("")

				srv.Handler.ServeHTTP(recorder, request)

				resp := decodeTestResponse[handlers.ErrorResponse](recorder.Body)
				Expect(resp).To(Equal(&handlers.ErrorResponse{Error: "invalid parameters"}))
			})
		})
		When("no body provided", func() {
			It("will return 400", func() {
				request, _ := http.NewRequest("POST", "/shorten", nil)

				srv.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusBadRequest))
			})
			It("will tell you invalid parameters", func() {
				request, _ := http.NewRequest("POST", "/shorten", nil)

				srv.Handler.ServeHTTP(recorder, request)

				resp := decodeTestResponse[handlers.ErrorResponse](recorder.Body)
				Expect(resp).To(Equal(&handlers.ErrorResponse{Error: "invalid parameters"}))
			})
		})
		When("an insertion error occurs", func() {
			It("returns a 500", func() {
				request, _ := newShortenedRequest("blah")

				store.hasCreateError = true

				srv.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusInternalServerError))
			})
			It("tells you there is a server error", func() {
				request, _ := newShortenedRequest("blah")

				store.hasCreateError = true

				srv.Handler.ServeHTTP(recorder, request)

				resp := decodeTestResponse[handlers.ErrorResponse](recorder.Body)
				Expect(resp).To(Equal(&handlers.ErrorResponse{Error: "server error"}))
			})
		})
		When("a cache error occurs", func() {
			It("returns a 500", func() {
				request, _ := newShortenedRequest("blah")

				cache.hasSetError = true

				srv.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusInternalServerError))
			})
			It("tells you there is a server error", func() {
				request, _ := newShortenedRequest("blah")

				cache.hasSetError = true

				srv.Handler.ServeHTTP(recorder, request)

				resp := decodeTestResponse[handlers.ErrorResponse](recorder.Body)
				Expect(resp).To(Equal(&handlers.ErrorResponse{Error: "server error"}))
			})
		})
	})
	Context("visiting a shortened link", func() {
		When("a valid link", func() {
			var targetHash, targetURL string
			BeforeEach(func() {
				targetHash = "abc"
				targetURL = "http://example.com"
				cache.Set(targetHash, targetURL, 1)
			})
			It("returns a 302", func() {
				request, _ := http.NewRequest("GET", "/v/"+targetHash, nil)

				srv.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusFound))
			})
			It("sets the Location header", func() {
				request, _ := http.NewRequest("GET", "/v/"+targetHash, nil)

				srv.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Header().Get("Location")).To(Equal(targetURL))
			})
			It("creates a visit record", func() {
				request, _ := http.NewRequest("GET", "/v/"+targetHash, nil)

				srv.Handler.ServeHTTP(recorder, request)
				// TODO: best assertion
				Expect(len(store.Visits) > 0).To(BeTrue())
			})
		})
		When("link not in cache", func() {
			When("it exists in the database", func() {
				var targetHash, targetURL string
				BeforeEach(func() {
					targetHash = "abc"
					targetURL = "http://example.com"
					store.CreateShortenedRecord(targetHash, targetURL)
				})

				It("will redirect", func() {
					request, _ := http.NewRequest("GET", "/v/"+targetHash, nil)

					srv.Handler.ServeHTTP(recorder, request)

					Expect(recorder.Code).To(Equal(http.StatusFound))
					Expect(recorder.Header().Get("Location")).To(Equal(targetURL))
				})
				It("will cache the value", func() {
					request, _ := http.NewRequest("GET", "/v/"+targetHash, nil)

					srv.Handler.ServeHTTP(recorder, request)

					obs, _ := cache.Get(targetHash)
					Expect(obs).To(Equal(targetURL))
				})
			})
			It("returns a 404", func() {
				request, _ := http.NewRequest("GET", "/v/abc", nil)

				srv.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusNotFound))
			})
		})
	})
})

func decodeTestResponse[T any](b io.Reader) *T {
	GinkgoHelper()
	var v T
	if err := json.NewDecoder(b).Decode(&v); err != nil {
		Fail(fmt.Sprintf("error decoding response: %s", err))
	}
	return &v
}

func encodeTestRequest[T any](data *T) io.ReadWriter {
	GinkgoHelper()
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(data); err != nil {
		Fail(fmt.Sprintf("failed to encode request: %s", err))
	}
	return &b
}

type FakeHasher struct {
}

func (f *FakeHasher) Encode(input string) string { return input + "+hello" }
func (f *FakeHasher) Decode(input string) string { return "" }

type FakeDataStore struct {
	Data           map[string]*handlers.ShortenedRecord
	hasCreateError bool
	Visits         map[int]*handlers.VisitRecord
}

func (f *FakeDataStore) CreateShortenedRecord(
	shortened, original string,
) (*handlers.ShortenedRecord, error) {
	if f.hasCreateError {
		return nil, errors.New("failed to create record")
	}
	f.Data[shortened] = &handlers.ShortenedRecord{
		Id:        1,
		Encoded:   shortened,
		TargetURL: original,
		CreatedAt: time.Now(),
	}
	return f.Data[shortened], nil
}

func (f *FakeDataStore) Get(key string) (*handlers.ShortenedRecord, bool) {
	val, ok := f.Data[key]
	return val, ok
}

func (f *FakeDataStore) CreateVisitRecord(shortId int) (*handlers.VisitRecord, error) {
	v := &handlers.VisitRecord{
		Id:        1,
		ShortId:   shortId,
		CreatedAt: time.Now(),
	}
	f.Visits[v.Id] = v
	return v, nil
}

type FakeCacheStore struct {
	Cache       map[string]string
	hasSetError bool
}

func (f *FakeCacheStore) Set(key, value string, expiration time.Duration) error {
	if f.hasSetError {
		return errors.New("error setting cache")
	}
	f.Cache[key] = value
	return nil
}

func (f *FakeCacheStore) Get(key string) (string, bool) {
	val, ok := f.Cache[key]
	return val, ok
}

func newShortenedRequest(url string) (*http.Request, *handlers.ShortenRequest) {
	data := &handlers.ShortenRequest{URL: url}
	body := encodeTestRequest[handlers.ShortenRequest](data)
	request, _ := http.NewRequest("POST", "/shorten", body)
	return request, data
}
