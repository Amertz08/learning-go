package url_shortener

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAPI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "API tests")
}

type ShortenedResponse struct {
	URL string `json:"url"`
}

func encodeResponse(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func decodeTestResponse[T any](b io.Reader) *T {
	GinkgoHelper()
	var v T
	if err := json.NewDecoder(b).Decode(&v); err != nil {
		Fail(fmt.Sprintf("error decoding response: %s", err))
	}
	return &v
}

func NewServer() *http.Server {
	mux := &http.ServeMux{}

	mux.HandleFunc("POST /shorten", func(w http.ResponseWriter, r *http.Request) {
		encodeResponse(w, http.StatusOK, &ShortenedResponse{URL: "blah"})
	})

	server := &http.Server{
		Handler: mux,
	}
	return server
}

var _ = Describe("Interacting with URL shortner API", func() {
	var srv *http.Server
	var recorder *httptest.ResponseRecorder

	BeforeEach(func() {
		srv = NewServer()
		recorder = httptest.NewRecorder()
	})
	When("creating a shortened link", func() {
		It("returns a 200", func() {
			request, _ := http.NewRequest("POST", "/shorten", nil)

			srv.Handler.ServeHTTP(recorder, request)

			Expect(recorder.Code).To(Equal(http.StatusOK))

		})
		It("returns the url", func() {
			request, _ := http.NewRequest("POST", "/shorten", nil)

			srv.Handler.ServeHTTP(recorder, request)

			resp := decodeTestResponse[ShortenedResponse](recorder.Body)
			Expect(resp.URL).To(Equal("blah"))
		})
	})
})
