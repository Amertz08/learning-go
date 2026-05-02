package datastructures

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLinkedList(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Linked List")
}

var _ = Describe("We are interacting with a linked list", func() {
	var uut LinkedList[int]
	var zeroVal int

	BeforeEach(func() {
		uut = NewLinkedList[int]()
	})

	When("the list is empty", func() {
		It("IsEmpty will return true", func() {
			Expect(uut.IsEmpty()).To(BeTrue())
		})
		It("Len returns zero", func() {
			Expect(uut.Len()).To(Equal(0))
		})
		It("Front will return false on 'ok' check", func() {
			_, ok := uut.Front()
			Expect(ok).To(BeFalse())
		})
		It("Front will return a nil value", func() {
			val, _ := uut.Front()
			Expect(val).To(BeNil())
		})
		It("Back will return false on 'ok' check", func() {
			_, ok := uut.Back()
			Expect(ok).To(BeFalse())
		})
		It("Back will return a nil value", func() {
			val, _ := uut.Back()
			Expect(val).To(BeNil())
		})
		It("PopFront will return false on 'ok' check", func() {
			_, ok := uut.PopFront()
			Expect(ok).To(BeFalse())
		})
		It("PopFront will return zero value", func() {
			val, _ := uut.PopFront()
			Expect(val).To(Equal(zeroVal))
		})
		It("PopBack will return false on 'ok' check", func() {
			_, ok := uut.PopBack()
			Expect(ok).To(BeFalse())
		})
		It("PopBack will return nil value", func() {
			val, _ := uut.PopBack()
			Expect(val).To(Equal(zeroVal))
		})
		Context("adding a value with PushFront", func() {
			pushedValue := 1

			BeforeEach(func() {
				uut.PushFront(pushedValue)
			})
			It("IsEmpty will return false", func() {
				Expect(uut.IsEmpty()).To(BeFalse())
			})
			It("Len will return 1", func() {
				Expect(uut.Len()).To(Equal(1))
			})
			It("Front will return true on 'ok' check", func() {
				_, ok := uut.Front()
				Expect(ok).To(BeTrue())
			})
			It("Front will return expected value", func() {
				observed, _ := uut.Front()
				Expect(observed.Value()).To(Equal(pushedValue))
			})
			It("Back will return true on 'ok' check", func() {
				_, ok := uut.Back()
				Expect(ok).To(BeTrue())
			})
			It("Back will return expected value", func() {
				observed, _ := uut.Back()
				Expect(observed.Value()).To(Equal(pushedValue))
			})
			It("Back and Front are the same node", func() {
				observedBack, _ := uut.Back()
				observedFront, _ := uut.Front()
				Expect(observedFront).To(BeIdenticalTo(observedBack))
			})
		})
	})
	When("list has one item", func() {
		BeforeEach(func() {
			uut.PushFront(1)
		})
		It("IsEmpty will return false", func() {
			Expect(uut.IsEmpty()).To(BeFalse())
		})
	})
})

func TestLinkedListImp_PushFront(t *testing.T) {
	t.Run("push multiple values", func(t *testing.T) {
		l := NewLinkedList[int]()

		exp := 2
		l.PushFront(1)
		l.PushFront(exp)

		front, ok := l.Front()
		if !ok {
			t.Fatalf("expected a value from front")
		}
		if l.Len() != 2 {
			t.Fatalf("expected a length of 2 got: %d", l.Len())
		}
		if front.Value() != exp {
			t.Fatalf("expected %d, got %d", exp, front.Value())
		}
	})
	t.Run("front is pointing at next in line", func(t *testing.T) {
		// TODO: we need the methods to do this
	})
}

func TestLinkedListImpl_PopFront(t *testing.T) {
	t.Run("one value case", func(t *testing.T) {
		l := NewLinkedList[int]()

		exp := 1

		l.PushFront(exp)

		obs, ok := l.PopFront()
		if !ok {
			t.Fatalf("expected ok")
		}
		if l.Len() != 0 {
			t.Fatalf("expected len 1 got: %d", l.Len())
		}
		if obs != exp {
			t.Errorf("expected: %d observed: %d", exp, obs)
		}
	})
	t.Run("new front assigned", func(t *testing.T) {
		l := NewLinkedList[int]()

		exp := 1

		l.PushFront(2)
		l.PushFront(exp)

		obs, ok := l.PopFront()
		if !ok {
			t.Fatalf("expected ok on pop front")
		}
		if obs != exp {
			t.Fatalf("expected: %d observed: %d", exp, obs)
		}
		front, ok := l.Front()
		if !ok {
			t.Fatalf("expected ok on front access")
		}
		if front.Value() != 2 {
			t.Fatalf("expected: 2 got %d", front.Value())
		}
		if l.Len() != 1 {
			t.Fatalf("expected len 1 got: %d", l.Len())
		}
	})
}

func TestLinkedListImpl_PushBack(t *testing.T) {
	t.Run("push a single value on an empty list", func(t *testing.T) {
		l := NewLinkedList[int]()

		exp := 1
		l.PushBack(exp)

		if l.Len() != 1 {
			t.Fatalf("expected a len of 1 got: %d", l.Len())
		}

		obs, ok := l.Back()

		if !ok {
			t.Fatalf("expected ok=false got true")
		}
		if obs.Value() != exp {
			t.Fatalf("got %d, want %d", obs.Value(), exp)
		}
		front, ok := l.Front()
		if !ok {
			t.Fatalf("expected a value at the front")
		}
		if front.Value() != obs.Value() {
			t.Errorf("expected front == back")
		}
	})
	t.Run("push multiple values", func(t *testing.T) {
		l := NewLinkedList[int]()

		exp := 1
		l.PushBack(2) // [2]
		back, ok := l.Back()
		if !ok {
			t.Fatalf("could not access back")
		}
		l.PushBack(exp) // [2,1]

		if l.Len() != 2 {
			t.Fatalf("expected len=2 got %d", l.Len())
		}

		oldBackNextNode, ok := l.Next(back)
		if !ok {
			t.Fatalf("could not retrieve next")
		}

		obs, ok := l.Back()
		if !ok {
			t.Fatalf("expected ok=true got false")
		}
		if oldBackNextNode != obs {
			t.Fatalf("expected old back next node to be new back")
		}
		if obs.Value() != exp {
			t.Errorf("expected %d got %d", exp, obs.Value())
		}
	})
}

func TestLinkedListImpl_Back(t *testing.T) {
	t.Run("one value", func(t *testing.T) {
		l := NewLinkedList[int]()

		exp := 1
		l.PushBack(exp)

		val, ok := l.Back()

		if !ok {
			t.Fatalf("expected ok=true got false")
		}
		if val.Value() != exp {
			t.Errorf("expected %d got %d", exp, val.Value())
		}
	})
	t.Run("push multiple values", func(t *testing.T) {
		l := NewLinkedList[int]()

		exp := 1
		l.PushBack(2)
		l.PushBack(exp)

		val, ok := l.Back()

		if !ok {
			t.Fatalf("expected ok=true got false")
		}
		if val.Value() != exp {
			t.Fatalf("expected %d got %d", exp, val.Value())
		}
	})
}

func TestLinkedListImpl_PopBack(t *testing.T) {
	t.Run("single item", func(t *testing.T) {
		l := NewLinkedList[int]()
		exp := 1
		l.PushBack(exp)

		obs, ok := l.PopBack()
		if !ok {
			t.Fatalf("expected ok=true got false")
		}
		if l.Len() != 0 {
			t.Fatalf("expected len=0 got %d", l.Len())
		}
		if obs != exp {
			t.Fatalf("expected %d got %d", exp, obs)
		}
		front, ok := l.Front()
		if ok {
			t.Fatalf("expected ok=true got false on front call")
		}
		if front != nil {
			t.Fatalf("expected front to be nil")
		}
	})
	t.Run("multiple items", func(t *testing.T) {
		l := NewLinkedList[int]()
		firstVal := 1
		secondVal := 2
		l.PushBack(firstVal)
		l.PushBack(secondVal)

		obs, ok := l.PopBack()
		if !ok {
			t.Fatalf("expected ok=true got false for %d", secondVal)
		}
		if obs != secondVal {
			t.Fatalf("expected %d got %d", secondVal, obs)
		}
		if l.Len() != 1 {
			t.Fatalf("expected len=1 got %d", l.Len())
		}
		back, ok := l.Back()
		if !ok {
			t.Fatalf("expected ok=true got false for %d", firstVal)
		}
		if l.Len() != 1 {
			t.Fatalf("expected len=1 got %d", l.Len())
		}
		if back.Value() != firstVal {
			t.Fatalf("expected %d got %d", firstVal, obs)
		}
	})
}
