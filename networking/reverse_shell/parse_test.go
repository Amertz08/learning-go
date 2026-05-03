package main

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestQueue(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Queue")
}

var _ = Describe("parse command input", func() {
	It("will return a single command correctly", func() {
		result := parse("ls")
		Expect(result).To(Equal([]string{"ls"}))
	})
	It("will handle correct spaces", func() {
		result := parse("ls -l")
		Expect(result).To(Equal([]string{"ls", "-l"}))
	})
	It("will handle excess spacing", func() {
		result := parse("ls  -l")
		Expect(result).To(Equal([]string{"ls", "-l"}))
	})
})
