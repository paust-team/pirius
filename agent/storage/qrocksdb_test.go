package storage_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/linxGnu/grocksdb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/paust-team/shapleq/agent/storage"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

var _ = Describe("Qrocksdb", func() {

	Context("RecordKey", func() {
		var recordKey *storage.RecordKey

		Describe("Generating new RecordKey", func() {
			expectedTopic := "test_topic"
			var expectedOffset uint64 = 1
			var expectedFragmentId uint32 = 2

			BeforeEach(func() {
				recordKey = storage.NewRecordKeyFromData(expectedTopic, expectedFragmentId, expectedOffset)
			})

			It("should be equal", func() {
				Expect(recordKey.Topic()).To(Equal(expectedTopic))
				Expect(recordKey.Offset()).To(Equal(expectedOffset))
				Expect(recordKey.FragmentId()).To(Equal(expectedFragmentId))
			})
		})
	})

	Context("RetentionPeriodKey", func() {
		var retentionKey *storage.RetentionPeriodKey

		Describe("Generating new RetentionPeriodKey", func() {
			expectedTopic := "test_topic"
			var expectedOffset uint64 = 1
			var expectedFragmentId uint32 = 2
			expectedExpirationDate := storage.GetNowTimestamp() + 10

			BeforeEach(func() {
				retentionKey = storage.NewRetentionPeriodKeyFromData(
					storage.NewRecordKeyFromData(expectedTopic, expectedFragmentId, expectedOffset), expectedExpirationDate)
			})

			It("should be equal", func() {
				Expect(retentionKey.ExpirationDate()).To(Equal(expectedExpirationDate))
				Expect(retentionKey.RecordKey().Topic()).To(Equal(expectedTopic))
				Expect(retentionKey.RecordKey().Offset()).To(Equal(expectedOffset))
				Expect(retentionKey.RecordKey().FragmentId()).To(Equal(expectedFragmentId))
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
				var err error
				var record *grocksdb.Slice
				BeforeEach(func() {
					record, err = db.GetRecord("non-exists-topic", 0, 0)
					Expect(err).NotTo(HaveOccurred())
				})
				AfterEach(func() {
					record.Free()
				})

				It("should have nil data", func() {
					Expect(record.Data()).To(BeNil())
				})
			})
			When("the record exists", func() {
				expected := []byte{1, 2, 3, 4, 5}
				topic := "test_topic"
				var fragmentId uint32 = 1
				var seqNum uint64 = 10
				expirationDate := storage.GetNowTimestamp() + 10
				var recordValue *storage.RecordValue

				BeforeEach(func() {
					err = db.PutRecord(topic, fragmentId, 0, seqNum, expected, expirationDate)
					Expect(err).NotTo(HaveOccurred())

					record, err := db.GetRecord(topic, fragmentId, 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(record.Data()).NotTo(BeNil())
					recordValue = storage.NewRecordValue(record)
				})
				AfterEach(func() {
					recordValue.Free()
				})

				It("should fetch same record value", func() {
					Expect(recordValue.PublishedData()).To(Equal(expected))
					Expect(recordValue.SeqNum()).To(Equal(seqNum))
				})
			})
		})

		Describe("Deleting expired records", func() {
			var offsetShouldBeRemained uint64 = 0
			var offsetShouldBeDeleted uint64 = 1
			var deletedCount int
			var record *grocksdb.Slice
			topic := "test_topic"
			var fragmentId uint32 = 1

			BeforeEach(func() {
				shortExpirationDate := storage.GetNowTimestamp() + 1
				longExpirationDate := storage.GetNowTimestamp() + 10

				err = db.PutRecord(topic, fragmentId, offsetShouldBeRemained, 1, []byte{1}, longExpirationDate)
				Expect(err).NotTo(HaveOccurred())
				err = db.PutRecord(topic, fragmentId, offsetShouldBeDeleted, 1, []byte{2}, shortExpirationDate)
				Expect(err).NotTo(HaveOccurred())

				time.Sleep(2 * time.Second)
				deletedCount, err = db.DeleteExpiredRecords()
				Expect(err).NotTo(HaveOccurred())
			})
			AfterEach(func() {
				if record != nil {
					record.Free()
				}
			})

			It("only expired records are deleted", func() {
				Expect(deletedCount).To(Equal(1))
			})
			It("can fetch non-expired record", func() {
				record, err = db.GetRecord(topic, fragmentId, offsetShouldBeRemained)
				Expect(err).NotTo(HaveOccurred())
				Expect(record.Data()).NotTo(BeNil())
			})
			It("cannot fetch expired record", func() {
				record, err = db.GetRecord(topic, fragmentId, offsetShouldBeDeleted)
				Expect(err).NotTo(HaveOccurred())
				Expect(record.Data()).To(BeNil())
			})
		})

		Describe("Iterating topic records", Ordered, func() {
			topic := "test_topic2"
			var fragmentId uint32 = 1
			expirationDate := storage.GetNowTimestamp() + 10
			var prefix []byte
			totalStoredNumOffset := 1000
			var it *grocksdb.Iterator

			BeforeAll(func() {
				prefix = make([]byte, len(topic)+1+int(unsafe.Sizeof(uint32(0))))
				copy(prefix, topic+"@")
				binary.BigEndian.PutUint32(prefix[len(topic)+1:], fragmentId)
			})
			BeforeEach(func() {
				it = db.Scan(storage.RecordCF)
				for i := 0; i < totalStoredNumOffset; i++ {
					err := db.PutRecord(topic, fragmentId, uint64(i), 0, []byte(fmt.Sprintf("data%d", i)), expirationDate)
					Expect(err).NotTo(HaveOccurred())
				}
			})

			When("iterating old records", func() {
				var startOffset uint64 = 0
				prevKey := storage.NewRecordKeyFromData(topic, fragmentId, startOffset)
				var receivedData [][]byte

				BeforeEach(func() {
					for it.Seek(prevKey.Data()); it.Valid() && bytes.HasPrefix(it.Key().Data(), prefix); it.Next() {
						key := storage.NewRecordKey(it.Key())
						if key.Offset() != startOffset {
							break
						}
						value := storage.NewRecordValue(it.Value())
						receivedData = append(receivedData, value.PublishedData())
						startOffset++
						prevKey.SetOffset(startOffset)
					}
				})

				It("iterated all stored records", func() {
					Expect(receivedData).To(HaveLen(totalStoredNumOffset))
				})
			})

			When("iterating live records", func() {
				var startOffset = uint64(totalStoredNumOffset)
				prevKey := storage.NewRecordKeyFromData(topic, fragmentId, startOffset)
				var receivedData [][]byte
				newlyAddedOffsets := 2000

				BeforeEach(func() {
					wg := sync.WaitGroup{}
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := totalStoredNumOffset; i < totalStoredNumOffset+newlyAddedOffsets; i++ {
							err := db.PutRecord(topic, fragmentId, uint64(i), 0, []byte(fmt.Sprintf("data%d", i)), expirationDate)
							Expect(err).NotTo(HaveOccurred())
							time.Sleep(1 * time.Millisecond)
						}
					}()

					wg.Add(1)
					go func() {
						defer wg.Done()

						for {
							for it.Seek(prevKey.Data()); it.Valid() && bytes.HasPrefix(it.Key().Data(), prefix); it.Next() {
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
							if len(receivedData) == newlyAddedOffsets {
								break
							}
							// wait for iterator to be updated
							time.Sleep(10 * time.Millisecond)
						}
					}()
					wg.Wait()
				})

				It("iterated all new records", func() {
					Expect(receivedData).To(HaveLen(newlyAddedOffsets))
				})
			})
		})
	})
})
