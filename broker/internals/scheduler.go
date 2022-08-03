package internals

import (
	"context"
	"github.com/paust-team/shapleq/broker/storage"
	logger "github.com/paust-team/shapleq/log"
	"time"
)

type RetentionScheduler struct {
	db            *storage.QRocksDB
	checkInterval time.Duration
	logger        *logger.QLogger
}

// NewRetentionScheduler interval is milliseconds
func NewRetentionScheduler(db *storage.QRocksDB, interval uint) *RetentionScheduler {
	return &RetentionScheduler{
		db:            db,
		logger:        logger.NewQLogger("RetentionScheduler", logger.Info),
		checkInterval: time.Millisecond * time.Duration(interval),
	}
}
func (r *RetentionScheduler) SetLogLevel(logLevel logger.LogLevel) {
	r.logger.SetLogLevel(logLevel)
}

func (r *RetentionScheduler) Run(ctx context.Context) {
	go func() {
		timer := time.NewTimer(r.checkInterval)
		defer timer.Stop()

		r.logger.Info("Start RetentionScheduler")
		for {
			select {
			case <-ctx.Done():
				r.logger.Info("RetentionScheduler is stopped from ctx.Done().")
				return
			case <-timer.C:
				deletedCount, err := r.db.DeleteExpiredRecords()
				if deletedCount > 0 {
					r.logger.Infof("%d records are deleted.", deletedCount)
				}

				if err != nil {
					r.logger.Error(err)
				}
			}
			timer.Reset(r.checkInterval)
		}
	}()
}
