package rebalancing_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRebalancing(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rebalancing Suite")
}
