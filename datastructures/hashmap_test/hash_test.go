package hashmap_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/datastructures"
)

func TestHashMap(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "HashMap tests")
}

var _ = Describe("bash hash map tests", func() {
	var uut datastructures.HashMap[int]

	BeforeEach(func() {
		uut = datastructures.NewHashMap[int]()
	})
	When("dealing with an empty hashmap", func() {
		It("does not return an error", func() {
			key := "hello"
			value := 5
			Expect(uut.Put(key, value)).To(BeNil())
		})
		It("can get the value back", func() {
			key := "hello"
			value := 5

			uut.Put(key, value)
			val, ok := uut.Get(key)

			Expect(ok).To(BeTrue())
			Expect(val).To(Equal(value))
		})
		It("will not return key that does not exist", func() {
			val, ok := uut.Get("nope")
			Expect(ok).To(BeFalse())
			Expect(val).To(Equal(0))
		})
	})
})
