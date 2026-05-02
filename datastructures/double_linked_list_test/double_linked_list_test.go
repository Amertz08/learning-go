package double_linked_list_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/datastructures"
)

func TestDoubleLinkedList(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Double Linked List")
}

var _ = Describe("Double linked list", func() {
	var uut *datastructures.DoubleLinkedList

	BeforeEach(func() {
		uut = datastructures.NewDoubleLinkedList()
	})

	When("list is empty", func() {
		It("will have a length of zero", func() {
			Expect(uut.Size()).To(Equal(0))
		})
		It("will tell you it is empty", func() {
			Expect(uut.IsEmpty()).To(BeTrue())
		})
		It("will return nil for the front", func() {
			Expect(uut.Front()).To(BeNil())
		})
		It("will return nil for the back", func() {
			Expect(uut.Back()).To(BeNil())
		})
		It("will return an error when you attempt to remove a value", func() {
			err := uut.Remove(2)
			Expect(err).Should(HaveOccurred())
		})
	})
})

func TestSize(t *testing.T) {
	t.Run("empty_list", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		if l.Size() != 0 {
			t.Errorf("Expected size of empty list to be 0, got %d", l.Size())
		}
	})
	t.Run("only_one_element", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		l.PushEnd(1)
		if l.Size() != 1 {
			t.Errorf("Expected size of list with 1 element to be 1, got %d", l.Size())
		}
	})
	t.Run("non_empty_list", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		l.PushEnd(1)
		l.PushEnd(2)
		if l.Size() != 2 {
			t.Errorf("Expected size of list with 2 elements to be 2, got %d", l.Size())
		}
	})
}

func TestIsEmpty(t *testing.T) {
	t.Run("empty_list", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		if !l.IsEmpty() {
			t.Errorf("Expected empty list to be reported as empty, got non-empty")
		}
	})
	t.Run("non_empty_list", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		l.PushEnd(1)
		if l.IsEmpty() {
			t.Errorf("Expected non-empty list to be reported as non-empty, got empty")
		}
	})
}

func TestFront(t *testing.T) {
	t.Run("empty_list", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		if l.Front() != nil {
			t.Errorf("Expected front of empty list to be nil, got %d", l.Front().Val)
		}
	})
	t.Run("non_empty_list", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		l.PushEnd(1)
		if l.Front() == nil {
			t.Errorf("Expected front of non-empty list to be set, got nil")
		}
		if l.Front().Val != 1 {
			t.Errorf("Expected front value to be 1, got %d", l.Front().Val)
		}
	})
}

func TestBack(t *testing.T) {
	t.Run("empty_list", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		if l.Back() != nil {
			t.Errorf("Expected back of empty list to be nil, got %d", l.Back().Val)
		}
	})
	t.Run("non_empty_list", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		l.PushEnd(1)
		if l.Back() == nil {
			t.Errorf("Expected back of non-empty list to be set, got nil")
		}
		if l.Back().Val != 1 {
			t.Errorf("Expected back value to be 1, got %d", l.Back().Val)
		}
	})
	t.Run("multiple_values", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		l.PushEnd(1)
		l.PushEnd(2)
		if l.Back() == nil {
			t.Errorf("Expected back of non-empty list to be set, got nil")
		}
		if l.Back().Val != 2 {
			t.Errorf("Expected back value to be 2, got %d", l.Back().Val)
		}
	})
}

func TestPushEnd(t *testing.T) {
	t.Run("append_single_element", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		l.PushEnd(1)

		if l.Front() == nil {
			t.Errorf("Expected head to be set after append, got nil")
		}
		if l.Front().Val != 1 {
			t.Errorf("Expected head value to be 1, got %d", l.Front().Val)
		}
	})
	t.Run("append_multiple_elements", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		l.PushEnd(1)
		l.PushEnd(2)

		if l.Front() == nil {
			t.Errorf("Expected head to be set after append, got nil")
		}
		if l.Front().Val != 1 {
			t.Errorf("Expected head value to be 1, got %d", l.Front().Val)
		}
		if l.Front().Next == nil {
			t.Errorf("Expected second node to be set after append, got nil")
		}
		if l.Front().Next.Val != 2 {
			t.Errorf("Expected second node value to be 2, got %d", l.Front().Next.Val)
		}
	})
}

func TestPushFront(t *testing.T) {
	t.Run("empty_list", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		l.PushFront(1)
		if l.Front() == nil {
			t.Errorf("Expected head to be set after push front, got nil")
		}
		if l.Front().Val != 1 {
			t.Errorf("Expected head value to be 1, got %d", l.Front().Val)
		}
	})
	t.Run("already_has_value", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		l.PushFront(1)
		l.PushFront(2)

		if l.Front() == nil {
			t.Errorf("Expected head to be set after push front, got nil")
		}
		if l.Front().Val != 2 {
			t.Errorf("Expected head value to be 2, got %d", l.Front().Val)
		}
	})
}

func TestRemove(t *testing.T) {
	t.Run("remove_from_empty_list", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		err := l.Remove(1)
		if err == nil {
			t.Errorf("Expected error when removing from empty list, got nil")
		}
	})
	t.Run("remove_single_element", func(t *testing.T) {
		l := datastructures.NewDoubleLinkedList()
		l.PushFront(1)
		err := l.Remove(1)
		if err != nil {
			t.Errorf("Expected no error when removing single element, got %v", err)
		}
		if l.Front() != nil {
			t.Errorf("Expected head to be nil after remove, got %v", l.Front())
		}
	})
}
