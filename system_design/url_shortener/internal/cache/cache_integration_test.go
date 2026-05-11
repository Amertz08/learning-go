package cache

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/handlers"
)

func TestCacheIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cache integration")
}

var _ = Describe("cache integration tests", func() {
	When("setting a cache value", func() {
		var uut *RedisCache
		BeforeEach(func() {
			uut = NewRedisCache("localhost", "6379")
		})
		AfterEach(func() {
			err := uut.Close()
			Expect(err).To(BeNil())
		})
		It("will correctly set", func(ctx SpecContext) {
			err := uut.Set(
				ctx,
				"mykey",
				&handlers.ShortenedRecord{
					Id:        1,
					Encoded:   "abc",
					TargetURL: "http://example.com",
					CreatedAt: time.Now(),
				},
				1*time.Second,
			)
			Expect(err).To(BeNil())
		})
		It("will correctly get", func(ctx SpecContext) {
			input := handlers.ShortenedRecord{
				Id:        1,
				Encoded:   "abc",
				TargetURL: "http://example.com",
				CreatedAt: time.Now(),
			}
			err := uut.Set(
				ctx,
				"mykey",
				&input,
				1*time.Second,
			)
			Expect(err).To(BeNil())

			obs, err := uut.Get(ctx, "mykey")
			// TODO: compare whole struct
			Expect(err).To(BeNil())
			obsData := *obs
			Expect(obsData.Id).To(Equal(input.Id))
		})
	})
})
