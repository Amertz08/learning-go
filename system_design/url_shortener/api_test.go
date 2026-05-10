package url_shortener

import (
	"bytes"
	"encoding/json"
	"errors"
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

type ShortenRequest struct {
	URL string `json:"url"`
}
type ShortenedResponse struct {
	URL string `json:"url"`
}

func decodeRequest[T any](r io.ReadCloser) (*T, error) {
	var data T
	if err := json.NewDecoder(r).Decode(&data); err != nil {
		return nil, errors.New("could not decode request")
	}
	return &data, nil
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
		data, err := decodeRequest[ShortenRequest](r.Body)

		if err != nil {
			encodeResponse(w, http.StatusInternalServerError, nil)
			return
		}
		//	TODO: does the URL already exist?
		//	TODO: if exists return value
		//	TODO: if not exists, generate hash and store, then return value
		//	TODO: cache given read heavy work flow

		encodeResponse(w, http.StatusOK, &ShortenedResponse{URL: data.URL})
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
			data := &ShortenRequest{URL: "blah"}
			var b bytes.Buffer
			if err := json.NewEncoder(&b).Encode(data); err != nil {
				Fail("failed to encode data")
			}
			request, _ := http.NewRequest("POST", "/shorten", &b)

			srv.Handler.ServeHTTP(recorder, request)

			Expect(recorder.Code).To(Equal(http.StatusOK))

		})
		It("returns the url", func() {
			data := &ShortenRequest{URL: "blah"}
			var b bytes.Buffer
			if err := json.NewEncoder(&b).Encode(data); err != nil {
				Fail("failed to encode data")
			}
			request, _ := http.NewRequest("POST", "/shorten", &b)

			srv.Handler.ServeHTTP(recorder, request)

			resp := decodeTestResponse[ShortenedResponse](recorder.Body)
			Expect(resp.URL).To(Equal("blah"))
		})
	})
})
