// Async session teardown: upstream session deletes run off the response
// path through a bounded queue drained by a small worker pool. The chat
// handler enqueues after the client response is fully delivered; a full
// queue drops (logged) rather than blocking the chat path.
package server

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"simple-chat/internal/upstream"
)

// Async deletion defaults.
const (
	// DefaultDeleteQueueSize bounds pending delete jobs; overflow drops.
	DefaultDeleteQueueSize = 256
	// DefaultDeleteWorkers is the worker count. Deletes are I/O-bound and
	// rare relative to completions; two is enough headroom for bursts.
	DefaultDeleteWorkers = 2
	// DefaultDeleteTimeout bounds each delete attempt (each retry gets its own).
	DefaultDeleteTimeout = 15 * time.Second
	// DefaultDeleteDrainWait bounds graceful-shutdown drain.
	DefaultDeleteDrainWait = 10 * time.Second
)

// deleteJob is one session teardown.
type deleteJob struct {
	client    *upstream.Client
	token     string
	sessionID string
}

// deleterConfig sizes the deleter; zero values take the defaults.
type deleterConfig struct {
	queueSize int
	workers   int
	timeout   time.Duration
	drainWait time.Duration
}

func (c *deleterConfig) fillDefaults() {
	if c.queueSize <= 0 {
		c.queueSize = DefaultDeleteQueueSize
	}
	if c.workers <= 0 {
		c.workers = DefaultDeleteWorkers
	}
	if c.timeout <= 0 {
		c.timeout = DefaultDeleteTimeout
	}
	if c.drainWait <= 0 {
		c.drainWait = DefaultDeleteDrainWait
	}
}

// asyncDeleter runs delete jobs off the request path.
type asyncDeleter struct {
	cfg    deleterConfig
	logger *log.Logger

	queue    chan deleteJob
	closed   chan struct{} // closed exactly once by shutdown
	closeOne sync.Once     // guards close(closed)
	wg       sync.WaitGroup
}

// newAsyncDeleter starts the worker pool.
func newAsyncDeleter(cfg deleterConfig, logger *log.Logger) *asyncDeleter {
	cfg.fillDefaults()
	d := &asyncDeleter{
		cfg:    cfg,
		logger: logger,
		queue:  make(chan deleteJob, cfg.queueSize),
		closed: make(chan struct{}),
	}
	d.logger.Printf("async session deletion active (queue %d, workers %d)", cfg.queueSize, cfg.workers)
	for i := 0; i < cfg.workers; i++ {
		d.wg.Add(1)
		go d.worker()
	}
	return d
}

// enqueue offers a job without ever blocking the caller: a full queue (or a
// shutdown deleter) drops the job with a warning naming the session id.
func (d *asyncDeleter) enqueue(job deleteJob) {
	select {
	case <-d.closed:
		d.logger.Printf("warn: session delete dropped (shutting down): %s", job.sessionID)
		return
	default:
	}
	select {
	case d.queue <- job:
	case <-d.closed:
		d.logger.Printf("warn: session delete dropped (shutting down): %s", job.sessionID)
	default:
		d.logger.Printf("warn: session delete queue full, dropping delete for %s", job.sessionID)
	}
}

// worker drains the queue until shutdown.
func (d *asyncDeleter) worker() {
	defer d.wg.Done()
	for {
		select {
		case <-d.closed:
			// Drain what was already queued before shutdown was called.
			for {
				select {
				case job := <-d.queue:
					d.run(job)
				default:
					return
				}
			}
		case job := <-d.queue:
			d.run(job)
		}
	}
}

// run executes one delete: per-attempt timeout, exactly one retry on a
// transport error (upstream rejections — BizError — are not retried).
// Failures log at warn with the session id; never panics.
func (d *asyncDeleter) run(job deleteJob) {
	defer func() {
		if r := recover(); r != nil {
			d.logger.Printf("warn: session delete panic recovered for %s: %v", job.sessionID, r)
		}
	}()
	if err := d.attempt(job); err != nil {
		if !isTransportError(err) {
			d.logger.Printf("warn: session delete rejected for %s: %v", job.sessionID, err)
			return
		}
		// One retry on transport errors only.
		if err := d.attempt(job); err != nil {
			d.logger.Printf("warn: session delete failed for %s: %v", job.sessionID, err)
		}
	}
}

// attempt performs one DeleteSession call with its own timeout.
func (d *asyncDeleter) attempt(job deleteJob) error {
	ctx, cancel := context.WithTimeout(context.Background(), d.cfg.timeout)
	defer cancel()
	return job.client.DeleteSession(ctx, job.token, job.sessionID)
}

// isTransportError reports whether the failure is client-side (network,
// timeout, malformed response) rather than an upstream answer. BizError means
// the upstream responded and rejected the delete — retrying is wasted load.
func isTransportError(err error) bool {
	var be *upstream.BizError
	return !errors.As(err, &be)
}

// shutdown stops intake, drains the queue (bounded wait), and returns.
func (d *asyncDeleter) shutdown() {
	d.closeOne.Do(func() {
		close(d.closed)
	})
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d.cfg.drainWait):
		d.logger.Printf("warn: session delete drain timed out after %s; pending deletes abandoned", d.cfg.drainWait)
	}
	// Anything that raced into the queue after the workers exited is never
	// going to run — say so instead of losing it silently.
	for {
		select {
		case job := <-d.queue:
			d.logger.Printf("warn: session delete abandoned after shutdown: %s", job.sessionID)
		default:
			return
		}
	}
}
