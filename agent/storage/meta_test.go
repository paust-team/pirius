package storage_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/paust-team/shapleq/agent/storage"
	"os"
)

var _ = Describe("Meta", func() {
	testpath := os.ExpandEnv("test_ag_meta.gob")
	var agentMeta *storage.AgentMeta

	Describe("Updating a AgentMeta", func() {
		var loaded storage.AgentMeta
		var err error

		BeforeEach(func() {
			meta, err := storage.LoadAgentMeta(testpath)
			Expect(err).NotTo(HaveOccurred())
			agentMeta = &meta
		})
		AfterEach(func() {
			os.Remove(testpath)
		})

		When("updating the `IDs`", func() {
			newPubId := "updated-pub-id"
			newSubId := "updated-sub-id"
			BeforeEach(func() {
				agentMeta.PublisherID = newPubId
				agentMeta.SubscriberID = newSubId
				err = storage.SaveAgentMeta(testpath, *agentMeta)
				Expect(err).NotTo(HaveOccurred())

				loaded, err = storage.LoadAgentMeta(testpath)
				Expect(err).NotTo(HaveOccurred())
			})
			It("should be equal", func() {
				Expect(loaded.PublisherID).To(Equal(newPubId))
				Expect(loaded.SubscriberID).To(Equal(newSubId))
			})
		})

		When("updating the `Offsets`", func() {
			testTopic := "test-topic"
			newSubOffsets := map[uint]uint64{100: 10}
			newPubOffsets := map[uint]uint64{200: 20}

			BeforeEach(func() {
				agentMeta.SubscribedOffsets.Store(testTopic, newSubOffsets)
				agentMeta.PublishedOffsets.Store(testTopic, newPubOffsets)
				err = storage.SaveAgentMeta(testpath, *agentMeta)
				Expect(err).NotTo(HaveOccurred())

				loaded, err = storage.LoadAgentMeta(testpath)
				Expect(err).NotTo(HaveOccurred())
			})
			It("should be equal", func() {
				pubOffsets, ok := loaded.PublishedOffsets.Load(testTopic)
				Expect(ok).To(Equal(true))
				Expect(pubOffsets).To(Equal(newPubOffsets))

				subOffsets, ok := loaded.SubscribedOffsets.Load(testTopic)
				Expect(ok).To(Equal(true))
				Expect(subOffsets).To(Equal(newSubOffsets))
			})
		})
	})
})
