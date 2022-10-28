package storage

import (
	"context"
	"fmt"
	"github.com/paust-team/shapleq/logger"
	"time"
)

type RetentionScheduler struct {
	db            *QRocksDB
	checkInterval time.Duration
}

// NewRetentionScheduler interval is milliseconds
func NewRetentionScheduler(db *QRocksDB, interval uint) *RetentionScheduler {
	return &RetentionScheduler{
		db:            db,
		checkInterval: time.Millisecond * time.Duration(interval),
	}
}

func (r *RetentionScheduler) Run(ctx context.Context) {
	go func() {
		timer := time.NewTimer(r.checkInterval)
		defer timer.Stop()

		logger.Info("Start RetentionScheduler")
		for {
			select {
			case <-ctx.Done():
				logger.Info("RetentionScheduler is stopped from ctx.Done().")
				return
			case <-timer.C:
				deletedCount, err := r.db.DeleteExpiredRecords()
				if deletedCount > 0 {
					logger.Info(fmt.Sprintf("%d records are deleted.", deletedCount))
				}

				if err != nil {
					logger.Error(err.Error())
				}
			}
			timer.Reset(r.checkInterval)
		}
	}()
}
