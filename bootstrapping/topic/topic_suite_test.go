package topic_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTopic(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Topic Suite")
}
