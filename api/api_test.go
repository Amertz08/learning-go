package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/api/internal/handlers"
	"github.come/Amertz08/learning-go/api/internal/store"
)

func TestAPI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "API")
}

var _ = Describe("Interacting with API", func() {
	var server *http.Server
	var uut http.HandlerFunc
	var recorder *httptest.ResponseRecorder
	var logger *slog.Logger
	var userStore *store.InMemoryUserStore

	BeforeEach(func() {
		logger = initLogger(slog.LevelError)
		userStore = store.NewInMemoryUserStore()
		server = newServer(logger, "localhost", "8080", userStore)
		uut = server.Handler.ServeHTTP
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

				uut(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))
			})
			It("will return an empty response object", func() {
				request := newIndexRequest(nil)

				uut(recorder, request)

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

				uut(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))
			})
			It("will return the parameters given", func() {
				request := newIndexRequest(url.Values{
					"a": {"123"},
					"b": {"c"},
				})

				uut(recorder, request)

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

				uut(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))
			})
			It("will return the correct sum", func() {
				data := &handlers.SumRequest{A: new(int), B: new(int)}
				*data.A = 1
				*data.B = 2
				request := newSumRequest(data)

				uut(recorder, request)

				obs := decodeResponse[handlers.SumResponse](recorder.Body)
				Expect(obs.Sum).To(Equal(3))
			})
		})
		When("submitting an invalid create request", func() {
			It("will return a 400", func() {
				request := newSumRequest(&handlers.SumRequest{})

				uut(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusBadRequest))
			})
			It("will return a message", func() {
				request := newSumRequest(&handlers.SumRequest{})

				uut(recorder, request)

				obs := decodeResponse[handlers.ErrorResponse](recorder.Body)
				Expect(obs).To(Equal(&handlers.ErrorResponse{Error: "missing parameters"}))
			})
		})
		When("server errors occur", func() {

		})
	})
	Context("creating a user", func() {
		When("called with valid parameters", func() {
			It("will return a 200", func() {
				u := &handlers.UserRequest{First: new(string), Last: new(string)}
				*u.First = "adam"
				*u.Last = "mertz"

				request := newRequest[handlers.UserRequest]("POST", "/users", u, nil)

				uut(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))
			})
			It("will create the user", func() {
				u := &handlers.UserRequest{First: new(string), Last: new(string)}
				*u.First = "adam"
				*u.Last = "mertz"

				request := newRequest[handlers.UserRequest]("POST", "/users", u, nil)

				uut(recorder, request)

				_, ok := userStore.Data[*u.First+*u.Last]
				Expect(ok).To(BeTrue())
			})
		})
		When("called with invalid parameters", func() {
			It("will return a 400", func() {
				u := &handlers.UserRequest{}
				request := newRequest[handlers.UserRequest]("POST", "/users", u, nil)

				uut(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusBadRequest))
			})
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
