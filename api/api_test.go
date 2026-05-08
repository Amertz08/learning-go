package main

import (
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

	BeforeEach(func() {
		server = newServer("localhost", "8080")
	})
	It("has valid request", func() {
		request := newRequest("GET", "/", nil, map[string]string{"a": "B"})
		recorder := httptest.NewRecorder()

		server.Handler.ServeHTTP(recorder, request)

		body := recorder.Body.String()
		Expect(body).To(Equal("hello"))
	})
	It("try body request", func() {
		request := newRequest("POST", "/", nil, nil)

		Expect(request).ToNot(BeNil())
	})
	It("actual body", func() {
		request := newRequest("POST", "/", map[string]string{"a": "b"}, nil)
		Expect(request).ToNot(BeNil())
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
