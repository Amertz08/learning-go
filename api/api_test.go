package main

import (
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

	BeforeEach(func() {
		server = newServer("localhost", "8080")
	})
	It("has valid request", func() {
		request, _ := http.NewRequest("GET", "/", nil)
		recorder := httptest.NewRecorder()

		server.Handler.ServeHTTP(recorder, request)

		body := recorder.Body.String()
		Expect(body).To(Equal("hello"))
	})
})
