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
	When("inserting collisions", func() {
		It("will insert without issue", func() {
			keyOne := "hello"
			keyTwo := "hlloe"
			valueOne := 2
			valueTwo := 5

			uut.Put(keyOne, valueOne)
			uut.Put(keyTwo, valueTwo)

			val, ok := uut.Get(keyOne)
			Expect(ok).To(BeTrue())
			Expect(val).To(Equal(valueOne))

			val, ok = uut.Get(keyTwo)
			Expect(ok).To(BeTrue())
			Expect(val).To(Equal(valueTwo))
		})
	})
})
