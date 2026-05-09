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
	Context("calling the sum endpoint", func() {
		When("submitting a valid create request", func() {
			It("can decode the response", func() {
				request := newRequest("POST", "/sum", map[string]int{"a": 1, "b": 2}, nil)

				server.Handler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))
				var obs SumResponse
				json.NewDecoder(recorder.Body).Decode(&obs)
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
func newRequest(method, path string, bodyParams any, queryParams map[string]string) *http.Request {
	GinkgoHelper()
	var body io.Reader
	if bodyParams != nil {
		jsonData, err := json.Marshal(bodyParams)
		if err != nil {
			Fail(fmt.Sprintf("failed to marshall JSON %s\n", err))
		}
		body = bytes.NewBuffer(jsonData)
	}
	request, err := http.NewRequest(method, path, body)
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
