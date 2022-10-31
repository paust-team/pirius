package storage_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/paust-team/shapleq/agent/storage"
	"github.com/paust-team/shapleq/test"
	"os"
)

var _ = Describe("Meta", func() {

	Describe("Updating a AgentMeta", func() {
		tp := test.NewTestParams()
		var agentMeta *storage.AgentMeta

		BeforeEach(func() {
			tp.Set("testPath", os.ExpandEnv("test_ag_meta.gob"))

			meta, err := storage.LoadAgentMeta(tp.GetString("testPath"))
			Expect(err).NotTo(HaveOccurred())
			agentMeta = &meta
		})
		AfterEach(func() {
			os.Remove(tp.GetString("testPath"))
			tp.Clear()
		})

		When("updating the `IDs`", func() {
			BeforeEach(func() {
				tp.Set("newPubId", "updated-pub-id")
				tp.Set("newSubId", "updated-sub-id")

				agentMeta.PublisherID = tp.GetString("newPubId")
				agentMeta.SubscriberID = tp.GetString("newSubId")
				err := storage.SaveAgentMeta(tp.GetString("testPath"), *agentMeta)
				Expect(err).NotTo(HaveOccurred())
			})
			It("should be equal", func() {
				loaded, err := storage.LoadAgentMeta(tp.GetString("testPath"))
				Expect(err).NotTo(HaveOccurred())

				Expect(loaded.PublisherID).To(Equal(tp.GetString("newPubId")))
				Expect(loaded.SubscriberID).To(Equal(tp.GetString("newSubId")))
			})
		})

		When("updating the `Offsets`", func() {
			BeforeEach(func() {
				tp.Set("expTopic", "testTopic")
				tp.Set("expSubOffsets", map[uint]uint64{100: 10})
				tp.Set("expPubOffsets", map[uint]uint64{200: 20})

				agentMeta.SubscribedOffsets.Store(tp.GetString("expTopic"), tp.Get("expSubOffsets").(map[uint]uint64))
				agentMeta.PublishedOffsets.Store(tp.GetString("expTopic"), tp.Get("expPubOffsets").(map[uint]uint64))
				err := storage.SaveAgentMeta(tp.GetString("testPath"), *agentMeta)
				Expect(err).NotTo(HaveOccurred())
			})
			It("should be equal", func() {
				loaded, err := storage.LoadAgentMeta(tp.GetString("testPath"))
				Expect(err).NotTo(HaveOccurred())

				pubOffsets, ok := loaded.PublishedOffsets.Load(tp.GetString("expTopic"))
				Expect(ok).To(Equal(true))
				Expect(pubOffsets).To(Equal(tp.Get("expPubOffsets").(map[uint]uint64)))

				subOffsets, ok := loaded.SubscribedOffsets.Load(tp.GetString("expTopic"))
				Expect(ok).To(Equal(true))
				Expect(subOffsets).To(Equal(tp.Get("expSubOffsets").(map[uint]uint64)))
			})
		})
	})
})
