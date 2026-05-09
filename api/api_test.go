package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/api/internal/handlers"
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
		var newIndexRequest func(values url.Values) *http.Request
		BeforeEach(func() {
			newIndexRequest = func(qp url.Values) *http.Request {
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

				obs := decodeResponse[handlers.IndexResponse](recorder.Body)
				Expect(obs).ToNot(BeNil())
				Expect(obs).To(Equal(&handlers.IndexResponse{Params: url.Values{}}))
			})
		})
		When("query parameters provide", func() {
			It("will return a 200", func() {
				request := newIndexRequest(url.Values{
					"a": {"123"},
					"b": {"c"},
				})

				server.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))
			})
			It("will return the parameters given", func() {
				request := newIndexRequest(url.Values{
					"a": {"123"},
					"b": {"c"},
				})

				server.Handler.ServeHTTP(recorder, request)

				obs := decodeResponse[handlers.IndexResponse](recorder.Body)
				Expect(obs).ToNot(BeNil())
				Expect(obs).To(Equal(&handlers.IndexResponse{Params: url.Values{
					"a": {"123"},
					"b": {"c"},
				}}))
			})
		})
	})
	Context("calling the sum endpoint", func() {
		var newSumRequest func(*handlers.SumRequest) *http.Request

		BeforeEach(func() {
			newSumRequest = func(d *handlers.SumRequest) *http.Request {
				return newRequest[handlers.SumRequest]("POST", "/sum", d, nil)
			}
		})

		When("submitting a valid create request", func() {
			It("will return 200", func() {
				data := &handlers.SumRequest{A: new(int), B: new(int)}
				*data.A = 1
				*data.B = 2
				request := newSumRequest(data)

				server.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))
			})
			It("will return the correct sum", func() {
				data := &handlers.SumRequest{A: new(int), B: new(int)}
				*data.A = 1
				*data.B = 2
				request := newSumRequest(data)

				server.Handler.ServeHTTP(recorder, request)

				obs := decodeResponse[handlers.SumResponse](recorder.Body)
				Expect(obs.Sum).To(Equal(3))
			})
		})
		When("submitting an invalid create request", func() {
			It("will return a 400", func() {
				request := newSumRequest(&handlers.SumRequest{})

				server.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusBadRequest))
			})
			It("will return a message", func() {
				request := newSumRequest(&handlers.SumRequest{})

				server.Handler.ServeHTTP(recorder, request)

				obs := decodeResponse[handlers.ErrorResponse](recorder.Body)
				Expect(obs).To(Equal(&handlers.ErrorResponse{Error: "missing parameters"}))
			})
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
	queryParams url.Values,
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
	for k, valList := range queryParams {
		for _, v := range valList {
			q.Add(k, v)
		}
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
