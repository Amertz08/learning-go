package url_shortener

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/labstack/echo/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/cache"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/database"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/hasher"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/server"
)

func TestAPI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "API tests")
}

const DisableLogsLevel slog.Level = 20

var _ = Describe("Interacting with URL shortener API", func() {
	var srv *echo.Echo
	var recorder *httptest.ResponseRecorder
	var fakeHasher *hasher.FakeHasher
	var store *database.FakeDataStore
	var fakeCache *cache.FakeCacheStore
	var logger *slog.Logger

	BeforeEach(func() {
		recorder = httptest.NewRecorder()
		store = database.NewFakeDataStore()
		fakeHasher = hasher.NewFakeHasher()
		fakeCache = cache.NewFakeCacheStore()
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: DisableLogsLevel,
		}))

		srv = server.NewServer(logger, fakeHasher, store, fakeCache)
	})
	Context("creating a shortened link", func() {
		When("given valid parameters", func() {
			It("returns a 200", func() {
				request, _ := newShortenedRequest("blah")

				srv.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))

			})
			It("returns the url", func() {
				request, _ := newShortenedRequest("blah")

				srv.ServeHTTP(recorder, request)

				resp := decodeTestResponse[server.ShortenedResponse](recorder.Body)
				Expect(resp.URL).To(Equal("http://localhost:8080/v/blah+hello"))
			})
			It("stores the value in the database", func() {
				request, data := newShortenedRequest("blah")

				srv.ServeHTTP(recorder, request)

				encoded := fakeHasher.Encode(data.URL)
				Expect(store.Data[encoded].TargetURL).To(Equal(data.URL))
			})
			It("caches the value", func() {
				request, data := newShortenedRequest("blah")

				srv.ServeHTTP(recorder, request)

				encoded := fakeHasher.Encode(data.URL)
				Expect(fakeCache.Cache[encoded].TargetURL).To(Equal(data.URL))
			})
		})
		When("given a blank string", func() {
			It("will return 400", func() {
				request, _ := newShortenedRequest("")

				srv.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusBadRequest))
			})
			It("will tell you invalid parameters", func() {
				request, _ := newShortenedRequest("")

				srv.ServeHTTP(recorder, request)

				resp := decodeTestResponse[echo.HTTPError](recorder.Body)
				Expect(resp).To(Equal(&echo.HTTPError{Message: http.StatusText(http.StatusBadRequest)}))
			})
		})
		When("no body provided", func() {
			It("will return 400", func() {
				request, _ := http.NewRequest("POST", "/shorten", nil)

				srv.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusBadRequest))
			})
			It("will tell you invalid parameters", func() {
				request, _ := http.NewRequest("POST", "/shorten", nil)

				srv.ServeHTTP(recorder, request)

				resp := decodeTestResponse[echo.HTTPError](recorder.Body)
				Expect(resp).To(Equal(&echo.HTTPError{Message: http.StatusText(http.StatusBadRequest)}))
			})
		})
		When("an insertion error occurs", func() {
			It("returns a 500", func() {
				request, _ := newShortenedRequest("blah")

				store.HasCreateShortErr = true

				srv.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusInternalServerError))
			})
			It("tells you there is a server error", func() {
				request, _ := newShortenedRequest("blah")

				store.HasCreateShortErr = true

				srv.ServeHTTP(recorder, request)

				resp := decodeTestResponse[echo.HTTPError](recorder.Body)
				Expect(resp).To(Equal(&echo.HTTPError{Message: http.StatusText(http.StatusInternalServerError)}))
			})
		})
		When("a cache error occurs", func() {
			It("returns a 500", func() {
				request, _ := newShortenedRequest("blah")

				fakeCache.HasSetError = true

				srv.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusInternalServerError))
			})
			It("tells you there is a server error", func() {
				request, _ := newShortenedRequest("blah")

				fakeCache.HasSetError = true

				srv.ServeHTTP(recorder, request)

				resp := decodeTestResponse[echo.HTTPError](recorder.Body)
				Expect(resp).To(Equal(&echo.HTTPError{Message: http.StatusText(http.StatusInternalServerError)}))
			})
		})
	})
	Context("visiting a shortened link", func() {
		When("a valid link", func() {
			var targetHash, targetURL string
			BeforeEach(func() {
				targetHash = "abc"
				targetURL = "http://example.com"
				fakeCache.Set(
					nil,
					targetHash,
					&server.ShortenedRecord{Id: 1, Encoded: targetHash, TargetURL: targetURL},
					1,
				)
			})
			It("returns a 302", func() {
				request, _ := http.NewRequest("GET", "/v/"+targetHash, nil)

				srv.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusFound))
			})
			It("sets the Location header", func() {
				request, _ := http.NewRequest("GET", "/v/"+targetHash, nil)

				srv.ServeHTTP(recorder, request)

				Expect(recorder.Header().Get("Location")).To(Equal(targetURL))
			})
			It("creates a visit record", func() {
				request, _ := http.NewRequest("GET", "/v/"+targetHash, nil)

				srv.ServeHTTP(recorder, request)
				// TODO: better assertion
				Expect(len(store.Visits) > 0).To(BeTrue())
			})
		})
		When("link not in cache", func() {
			When("it exists in the database", func() {
				var targetHash, targetURL string
				BeforeEach(func(ctx SpecContext) {
					targetHash = "abc"
					targetURL = "http://example.com"
					_, err := store.CreateShortenedRecord(ctx, targetHash, targetURL)
					Expect(err).To(BeNil())
				})

				It("will redirect", func() {
					request, _ := http.NewRequest("GET", "/v/"+targetHash, nil)

					srv.ServeHTTP(recorder, request)

					Expect(recorder.Code).To(Equal(http.StatusFound))
					Expect(recorder.Header().Get("Location")).To(Equal(targetURL))
				})
				It("will cache the value", func() {
					request, _ := http.NewRequest("GET", "/v/"+targetHash, nil)

					srv.ServeHTTP(recorder, request)

					obs, _ := fakeCache.Get(nil, targetHash)
					Expect(obs.TargetURL).To(Equal(targetURL))
				})
			})
			It("returns a 404", func() {
				request, _ := http.NewRequest("GET", "/v/abc", nil)

				srv.ServeHTTP(recorder, request)

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

func newShortenedRequest(url string) (*http.Request, *server.ShortenRequest) {
	data := &server.ShortenRequest{URL: url}
	body := encodeTestRequest[server.ShortenRequest](data)
	request := httptest.NewRequest("POST", "/shorten", body)
	request.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	return request, data
}
