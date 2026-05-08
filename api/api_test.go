package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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
				request := newRequest("POST", "/sum", map[string]string{"a": "1", "b": "2"}, nil)

				server.Handler.ServeHTTP(recorder, request)

				type responseBody struct {
					Sum int `json:"sum"`
				}
				var obs responseBody
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

func newRequest(method, path string, bodyParams, queryParams map[string]string) *http.Request {
	form := url.Values{}
	for k, v := range bodyParams {
		form.Add(k, v)
	}
	request, _ := http.NewRequest(method, path, strings.NewReader(form.Encode()))
	q := request.URL.Query()
	for k, v := range queryParams {
		q.Add(k, v)
	}
	request.URL.RawQuery = q.Encode()
	return request
}
