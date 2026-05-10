package url_shortener

import (
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

func NewServer() *http.Server {
	mux := &http.ServeMux{}

	mux.HandleFunc("POST /shorten", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Handler: mux,
	}
	return server
}

var _ = Describe("Interacting with URL shortner API", func() {
	When("creating a shortened link", func() {
		It("returns a 200", func() {
			srv := NewServer()
			request, _ := http.NewRequest("POST", "/shorten", nil)
			recorder := httptest.NewRecorder()

			srv.Handler.ServeHTTP(recorder, request)

			Expect(recorder.Code).To(Equal(http.StatusOK))

		})
	})
})
