package storage_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/linxGnu/grocksdb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/paust-team/shapleq/agent/storage"
	"github.com/paust-team/shapleq/test"
	"runtime"
	"time"
	"unsafe"
)

var _ = Describe("Qrocksdb", func() {

	Context("RecordKey", func() {
		Describe("Generating new RecordKey", func() {
			tp := test.NewTestParams()

			BeforeEach(func() {
				tp.Set("expTopic", "test_topic")
				tp.Set("expOffset", uint64(1))
				tp.Set("expFragmentId", uint32(2))
			})

			It("should be equal", func() {
				recordKey := storage.NewRecordKeyFromData(
					tp.GetString("expTopic"),
					tp.GetUint32("expFragmentId"),
					tp.GetUint64("expOffset"))

				Expect(recordKey.Topic()).To(Equal(tp.GetString("expTopic")))
				Expect(recordKey.Offset()).To(Equal(tp.GetUint64("expOffset")))
				Expect(recordKey.FragmentId()).To(Equal(tp.GetUint32("expFragmentId")))
			})
		})
	})

	Context("RetentionPeriodKey", func() {
		Describe("Generating new RetentionPeriodKey", func() {
			tp := test.NewTestParams()

			BeforeEach(func() {
				tp.Set("expTopic", "test_topic")
				tp.Set("expOffset", uint64(1))
				tp.Set("expFragmentId", uint32(2))
				tp.Set("expExpirationDate", storage.GetNowTimestamp()+10)
			})

			It("should be equal", func() {
				retentionKey := storage.NewRetentionPeriodKeyFromData(
					storage.NewRecordKeyFromData(
						tp.GetString("expTopic"),
						tp.GetUint32("expFragmentId"),
						tp.GetUint64("expOffset")), tp.GetUint64("expExpirationDate"))

				Expect(retentionKey.ExpirationDate()).To(Equal(tp.GetUint64("expExpirationDate")))
				Expect(retentionKey.RecordKey().Topic()).To(Equal(tp.GetString("expTopic")))
				Expect(retentionKey.RecordKey().Offset()).To(Equal(tp.GetUint64("expOffset")))
				Expect(retentionKey.RecordKey().FragmentId()).To(Equal(tp.GetUint32("expFragmentId")))
			})
		})
	})

	Context("QRocksDB", Ordered, func() {
		var db *storage.QRocksDB
		var err error

		BeforeAll(func() {
			db, err = storage.NewQRocksDB("qstore", ".")
			Expect(err).NotTo(HaveOccurred())
		})
		AfterAll(func() {
			db.Close()
			db.Destroy()
		})

		Describe("Fetching a topic record", func() {
			When("the record not exists", func() {
				var record *grocksdb.Slice
				BeforeEach(func() {
					record, err = db.GetRecord("non-exists-topic", 0, 0)
					Expect(err).NotTo(HaveOccurred())
				})
				AfterEach(func() {
					record.Free()
				})

				It("must have nil data", func() {
					Expect(record.Data()).To(BeNil())
				})
			})
			When("the record exists", func() {
				tp := test.NewTestParams()
				BeforeEach(func() {
					tp.Set("expRecord", []byte{1, 2, 3, 4, 5})
					tp.Set("expTopic", "test_topic")
					tp.Set("expSeqNum", uint64(10))
					tp.Set("expFragmentId", uint32(1))
					tp.Set("expOffset", uint64(1))
					tp.Set("expExpirationDate", storage.GetNowTimestamp()+10)
					err = db.PutRecord(tp.GetString("expTopic"),
						tp.GetUint32("expFragmentId"),
						tp.GetUint64("expOffset"),
						tp.GetUint64("expSeqNum"),
						tp.GetBytes("expRecord"),
						tp.GetUint64("expExpirationDate"))
					Expect(err).NotTo(HaveOccurred())
				})

				It("must have same record value", func() {
					record, err := db.GetRecord(tp.GetString("expTopic"), tp.GetUint32("expFragmentId"), tp.GetUint64("expOffset"))
					Expect(err).NotTo(HaveOccurred())
					Expect(record.Data()).NotTo(BeNil())
					recordValue := storage.NewRecordValue(record)
					defer recordValue.Free()

					Expect(recordValue.PublishedData()).To(Equal(tp.GetBytes("expRecord")))
					Expect(recordValue.SeqNum()).To(Equal(tp.GetUint64("expSeqNum")))
				})
			})
		})

		Describe("Deleting expired record", Ordered, func() {
			tp := test.NewTestParams()
			var deletedCount int

			BeforeAll(func() {
				tp.Set("expTopic", "test_topic")
				tp.Set("expFragmentId", uint32(1))
				tp.Set("offsetShouldBeDeleted", uint64(1))
				tp.Set("offsetShouldBeRemained", uint64(0))
				tp.Set("shortExpirationDate", storage.GetNowTimestamp()+1)
				tp.Set("longExpirationDate", storage.GetNowTimestamp()+10000)

				err = db.PutRecord(tp.GetString("expTopic"),
					tp.GetUint32("expFragmentId"),
					tp.GetUint64("offsetShouldBeDeleted"),
					1,
					[]byte{1},
					tp.GetUint64("shortExpirationDate"))
				Expect(err).NotTo(HaveOccurred())

				err = db.PutRecord(tp.GetString("expTopic"),
					tp.GetUint32("expFragmentId"),
					tp.GetUint64("offsetShouldBeRemained"),
					1,
					[]byte{1},
					tp.GetUint64("longExpirationDate"))
				Expect(err).NotTo(HaveOccurred())

				time.Sleep(2 * time.Second)
				deletedCount, err = db.DeleteExpiredRecords()
				Expect(err).NotTo(HaveOccurred())
			})
			It("cannot delete again", func() {
				count, err := db.DeleteExpiredRecords()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(0))
			})
			It("only expired records are deleted", func() {
				Expect(deletedCount).To(Equal(1))
			})

			It("can fetch non-expired record", func() {
				record, err := db.GetRecord(tp.GetString("expTopic"), tp.GetUint32("expFragmentId"), tp.GetUint64("offsetShouldBeRemained"))
				defer record.Free()
				Expect(err).NotTo(HaveOccurred())
				Expect(record.Data()).NotTo(BeNil())
			})
			It("cannot fetch expired record", func() {
				record, err := db.GetRecord(tp.GetString("expTopic"), tp.GetUint32("expFragmentId"), tp.GetUint64("offsetShouldBeDeleted"))
				defer record.Free()
				Expect(err).NotTo(HaveOccurred())
				Expect(record.Data()).To(BeNil())
			})
		})

		Describe("Deleting expired records (ranged delete)", Ordered, func() {
			tp := test.NewTestParams()
			var deletedCount int

			BeforeAll(func() {
				tp.Set("expTopic", "test_topic3")
				tp.Set("expFragmentId", uint32(1))
				tp.Set("expireDate", storage.GetNowTimestamp()+1)
				tp.Set("count", 10)
				for i := 0; i < tp.GetInt("count"); i++ {
					err = db.PutRecord(tp.GetString("expTopic"),
						tp.GetUint32("expFragmentId"),
						uint64(i+1),
						uint64(i+1),
						[]byte{1},
						tp.GetUint64("expireDate"))
					Expect(err).NotTo(HaveOccurred())
				}

				time.Sleep(2 * time.Second)
				deletedCount, err = db.DeleteExpiredRecords()
				Expect(err).NotTo(HaveOccurred())
			})
			It("cannot delete again", func() {
				count, err := db.DeleteExpiredRecords()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(0))
			})
			It("all records are deleted", func() {
				Expect(deletedCount).To(Equal(tp.GetInt("count")))
			})
		})

		Describe("Iterating topic records", Ordered, func() {
			tp := test.NewTestParams()
			var it *grocksdb.Iterator

			BeforeEach(func() {
				tp.Set("expTopic", "test_topic2")
				tp.Set("expSeqNum", uint64(10))
				tp.Set("expFragmentId", uint32(1))
				tp.Set("expOffset", uint64(1))
				tp.Set("totNumOffset", 1000)
				tp.Set("expExpirationDate", storage.GetNowTimestamp()+10)
				prefix := make([]byte, len(tp.GetString("expTopic"))+1+int(unsafe.Sizeof(uint32(0))))
				copy(prefix, tp.GetString("expTopic")+"@")
				binary.BigEndian.PutUint32(prefix[len(tp.GetString("expTopic"))+1:], tp.GetUint32("expFragmentId"))
				tp.Set("prefix", prefix)

				it = db.Scan(storage.RecordCF)
				for i := 0; i < tp.GetInt("totNumOffset"); i++ {
					err = db.PutRecord(tp.GetString("expTopic"),
						tp.GetUint32("expFragmentId"),
						uint64(i),
						0,
						[]byte(fmt.Sprintf("data%d", i)),
						tp.GetUint64("expExpirationDate"))
					Expect(err).NotTo(HaveOccurred())
				}
			})
			AfterEach(func() {
				tp.Clear()
			})

			When("iterating old records", func() {
				It("can iterate all stored records", func() {
					var startOffset uint64 = 0
					var receivedData [][]byte
					prevKey := storage.NewRecordKeyFromData(tp.GetString("expTopic"), tp.GetUint32("expFragmentId"), startOffset)

					for it.Seek(prevKey.Data()); it.Valid() && bytes.HasPrefix(it.Key().Data(), tp.GetBytes("prefix")); it.Next() {
						key := storage.NewRecordKey(it.Key())
						if key.Offset() != startOffset {
							break
						}
						value := storage.NewRecordValue(it.Value())
						receivedData = append(receivedData, value.PublishedData())
						startOffset++
						prevKey.SetOffset(startOffset)
					}
					Expect(receivedData).To(HaveLen(tp.GetInt("totNumOffset")))
				})
			})

			When("iterating live records", func() {
				BeforeEach(func() {
					tp.Set("newlyAddedOffsets", 2000)
					go func() {
						for i := tp.GetInt("totNumOffset"); i < tp.GetInt("totNumOffset")+tp.GetInt("newlyAddedOffsets"); i++ {
							err = db.PutRecord(tp.GetString("expTopic"),
								tp.GetUint32("expFragmentId"),
								uint64(i),
								0,
								[]byte(fmt.Sprintf("data%d", i)),
								tp.GetUint64("expExpirationDate"))
							Expect(err).NotTo(HaveOccurred())
							time.Sleep(1 * time.Millisecond)
						}
					}()
				})

				It("can iterate all new records", func() {
					var receivedData [][]byte
					var startOffset = uint64(tp.GetInt("totNumOffset"))
					prevKey := storage.NewRecordKeyFromData(tp.GetString("expTopic"), tp.GetUint32("expFragmentId"), startOffset)

					for {
						for it.Seek(prevKey.Data()); it.Valid() && bytes.HasPrefix(it.Key().Data(), tp.GetBytes("prefix")); it.Next() {
							key := storage.NewRecordKey(it.Key())
							if key.Offset() != startOffset {
								break
							}
							value := storage.NewRecordValue(it.Value())
							receivedData = append(receivedData, value.PublishedData())
							startOffset++
							prevKey.SetOffset(startOffset)
							runtime.Gosched()
						}
						if len(receivedData) == tp.GetInt("newlyAddedOffsets") {
							break
						}
						// wait for iterator to be updated
						time.Sleep(10 * time.Millisecond)
					}
					Expect(receivedData).To(HaveLen(tp.GetInt("newlyAddedOffsets")))
				})
			})
		})
	})
})
