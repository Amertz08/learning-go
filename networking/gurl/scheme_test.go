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

var _ = Describe("has scheme checks", func() {
	DescribeTable("scheme cases", func(url string, expected bool) {
		Expect(hasScheme(url)).To(Equal(expected))
	},
		Entry("no scheme", "hello", false),
		Entry("http schema", "http://hello", true),
	)
})
