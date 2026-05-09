package main

import (
	"bytes"
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
	RunSpecs(t, "API")
}

var _ = Describe("Interacting with API", func() {
	var server *http.Server
	var recorder *httptest.ResponseRecorder

	BeforeEach(func() {
		server = newServer("localhost", "8080")
		recorder = httptest.NewRecorder()
	})
	Context("calling the index endpoint", func() {
		var newIndexRequest func(map[string]string) *http.Request
		BeforeEach(func() {
			newIndexRequest = func(qp map[string]string) *http.Request {
				return newRequest[any]("GET", "/", nil, qp)
			}
		})

		When("no query parameters", func() {
			It("will return 200", func() {
				request := newIndexRequest(nil)

				server.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))
			})
			It("will return an empty response object", func() {
				request := newIndexRequest(nil)

				server.Handler.ServeHTTP(recorder, request)

				obs := decodeResponse[IndexResponse](recorder.Body)
				Expect(obs).ToNot(BeNil())
				Expect(obs).To(Equal(&IndexResponse{Params: map[string][]string{}}))
			})
		})
		When("query parameters provide", func() {
			It("will return a 200", func() {
				request := newIndexRequest(map[string]string{
					"a": "123",
					"b": "c",
				})

				server.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))
			})
			It("will return the parameters given", func() {
				request := newIndexRequest(map[string]string{
					"a": "123",
					"b": "c",
				})

				server.Handler.ServeHTTP(recorder, request)

				obs := decodeResponse[IndexResponse](recorder.Body)
				Expect(obs).ToNot(BeNil())
				Expect(obs).To(Equal(&IndexResponse{Params: map[string][]string{
					"a": {"123"},
					"b": {"c"},
				}}))
			})
		})
	})
	Context("calling the sum endpoint", func() {
		When("submitting a valid create request", func() {
			It("can decode the response", func() {
				data := &SumRequest{A: 1, B: 2}
				request := newRequest[SumRequest]("POST", "/sum", data, nil)

				server.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))
				obs := decodeResponse[SumResponse](recorder.Body)
				Expect(obs.Sum).To(Equal(3))
			})
		})
		When("submitting an invalid create request", func() {

		})
		When("server errors occur", func() {

		})
	})
})

// newRequest creates a request. If bodyParams provided it will encode as JSON. If queryParams provided
// it will properly URL encode them.
func newRequest[T any](
	method, path string,
	bodyParams *T,
	queryParams map[string]string,
) *http.Request {
	GinkgoHelper()

	var buff bytes.Buffer
	if bodyParams != nil {
		if err := json.NewEncoder(&buff).Encode(bodyParams); err != nil {
			Fail(fmt.Sprintf("failed to create new request %s\n", err))
		}
	}

	request, err := http.NewRequest(method, path, &buff)
	if err != nil {
		Fail(fmt.Sprintf("failed to create new request %s\n", err))
	}
	if bodyParams != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	q := request.URL.Query()
	for k, v := range queryParams {
		q.Add(k, v)
	}
	request.URL.RawQuery = q.Encode()
	return request
}

// decodeResponse will JSON decode the response to the provided type or Fail the test
func decodeResponse[T any](r io.Reader) *T {
	GinkgoHelper()

	var resp T
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		Fail("failed to decode response")
	}
	return &resp
}
