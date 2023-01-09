package storage_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/paust-team/pirius/agent/storage"
	"github.com/paust-team/pirius/test"
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
				tp.Set("expSubFragId", uint(100))
				tp.Set("expPubFragId", uint(200))
				tp.Set("expFetFragId", uint(200))
				tp.Set("expSubOffsets", uint64(10))
				tp.Set("expPubOffsets", uint64(20))
				tp.Set("expFetOffsets", uint64(15))

				agentMeta.SubscribedOffsets.Store(
					storage.NewFragmentKey(tp.GetString("expTopic"), tp.GetUint("expSubFragId")), tp.GetUint64("expSubOffsets"))
				agentMeta.PublishedOffsets.Store(
					storage.NewFragmentKey(tp.GetString("expTopic"), tp.GetUint("expPubFragId")), tp.GetUint64("expPubOffsets"))
				agentMeta.LastFetchedOffset.Store(
					storage.NewFragmentKey(tp.GetString("expTopic"), tp.GetUint("expFetFragId")), tp.GetUint64("expFetOffsets"))
				err := storage.SaveAgentMeta(tp.GetString("testPath"), *agentMeta)

				Expect(err).NotTo(HaveOccurred())
			})
			It("should be equal", func() {
				loaded, err := storage.LoadAgentMeta(tp.GetString("testPath"))
				Expect(err).NotTo(HaveOccurred())

				pubOffsets, ok := loaded.PublishedOffsets.Load(storage.NewFragmentKey(tp.GetString("expTopic"), tp.GetUint("expPubFragId")))
				Expect(ok).To(Equal(true))
				Expect(pubOffsets).To(Equal(tp.Get("expPubOffsets").(uint64)))

				subOffsets, ok := loaded.SubscribedOffsets.Load(storage.NewFragmentKey(tp.GetString("expTopic"), tp.GetUint("expSubFragId")))
				Expect(ok).To(Equal(true))
				Expect(subOffsets).To(Equal(tp.Get("expSubOffsets").(uint64)))

				fetOffsets, ok := loaded.LastFetchedOffset.Load(storage.NewFragmentKey(tp.GetString("expTopic"), tp.GetUint("expFetFragId")))
				Expect(ok).To(Equal(true))
				Expect(fetOffsets).To(Equal(tp.Get("expFetOffsets").(uint64)))
			})
		})
	})
})
