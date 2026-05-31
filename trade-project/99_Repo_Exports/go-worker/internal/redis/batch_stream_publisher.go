package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"go-worker/infra/redisclient"
	"go-worker/internal/monitoring"
	"go-worker/internal/streams"

	"go.uber.org/zap"
)

// BatchStreamPublisher — батч-паблишер для Redis Streams.
//
// Зачем:
//   - liquidation events (forceOrder / allLiquidation) часто приходят пачками во время резких движений;
//     XADD на каждое событие создаёт лишнюю нагрузку и увеличивает tail-latency.
//   - батч стабилизирует задержку и снижает RPS на Redis.
//
// Поведение:
//   - Enqueue() кладёт запись в буфер.
//   - Фоновый флашер пишет в Redis каждые flushInterval или при достижении maxBatch.
//   - При переполнении буфера событие отбрасывается (fail-open), но caller обязан зафиксировать это метрикой.
//
// Важно:
//   - Это best-effort доставка. Если вам нужна гарантия без потерь, используйте consumer groups + durable
//     ingestion слой и/или отдельный брокер.
type BatchStreamPublisher struct {
	client        *goredis.Client
	stream        string
	maxLenApprox  int64
	maxBatch      int
	flushInterval time.Duration

	mu     sync.Mutex
	buffer []goredis.XAddArgs

	// maxBuffer — hard cap на размер буфера (в записях).
	// Если Redis недоступен, flush не сможет выгрузить данные, и буфер начнёт расти.
	// Чтобы не съесть всю память контейнера, вводим жёсткий предел.
	maxBuffer int

	triggerCh chan struct{}
	stopCh    chan struct{}
	once      sync.Once
}

// NewBatchStreamPublisher создаёт publisher для конкретного stream.
// maxLenApprox — примерный MAXLEN (~) для XADD.
func NewBatchStreamPublisher(client *goredis.Client, stream string, maxLenApprox int64, maxBatch int, flushInterval time.Duration) *BatchStreamPublisher {
	if maxBatch <= 0 {
		maxBatch = 100
	}
	if flushInterval <= 0 {
		flushInterval = 10 * time.Millisecond
	}
	if maxLenApprox <= 0 {
		maxLenApprox = streams.MaxLenGlobal
	}
	return &BatchStreamPublisher{
		client:        client,
		stream:        stream,
		maxLenApprox:  maxLenApprox,
		maxBatch:      maxBatch,
		flushInterval: flushInterval,
		buffer:        make([]goredis.XAddArgs, 0, maxBatch),
		maxBuffer:     maxBatch * 20,
		triggerCh:     make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
	}
}

func (p *BatchStreamPublisher) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(p.flushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				_ = p.Close(2 * time.Second)
				return
			case <-p.stopCh:
				return
			case <-p.triggerCh:
				if err := p.Flush(); err != nil {
					zap.S().Errorf("[batch_stream_publisher] Flush error: %v", err)
				}
			case <-ticker.C:
				if err := p.Flush(); err != nil {
					zap.S().Errorf("[batch_stream_publisher] Flush error: %v", err)
				}
			}
		}
	}()
}

// Enqueue добавляет запись в буфер.
// Возвращает error, если буфер переполнен, иначе nil.
func (p *BatchStreamPublisher) Enqueue(values map[string]interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.maxBuffer > 0 && len(p.buffer) >= p.maxBuffer {
		monitoring.RecordBatchPublisherDropped(p.stream)
		if p.client != nil {
			go func(s string, vals map[string]interface{}) {
				dlqCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				dlqStream := streams.DLQPrefix + s
				err := p.client.XAdd(dlqCtx, &goredis.XAddArgs{
					Stream: dlqStream,
					Values: vals,
					MaxLen: streams.MaxLenDLQ,
					Approx: true,
				}).Err()
				if err != nil {
					monitoring.RecordDLQWriteError(dlqStream)
					zap.S().Warnf("⚠️ DLQ write error stream=%s: %v", dlqStream, err)
				}
			}(p.stream, values)
		}
		return fmt.Errorf("buffer full for stream %s", p.stream)
	}

	args := goredis.XAddArgs{
		Stream: p.stream,
		Values: values,
		MaxLen: p.maxLenApprox,
		// Approx: true = XADD ... MAXLEN ~ N (тильда = приблизительное ограничение).
		// Redis выполняет trimming только когда накапливается достаточно записей для
		// удаления целого radix-tree node (~100 записей). В результате реальная длина
		// стрима может ВРЕМЕННО превышать maxLenApprox на ~10% при всплеске нагрузки
		// (burst), пока Redis не выполнит следующий trim.
		//
		// Это намеренное решение: точный trim (Approx: false) на порядок дороже по CPU.
		// При мониторинге длины стрима: алерт должен иметь запас ≥ 15% над maxLen,
		// чтобы не срабатывать на нормальный overshoot.
		Approx: true,
	}
	p.buffer = append(p.buffer, args)
	if len(p.buffer) >= p.maxBatch {
		select {
		case p.triggerCh <- struct{}{}:
		default:
		}
	}
	return nil
}

// Flush публикует текущий буфер в Redis.
func (p *BatchStreamPublisher) Flush() error {
	p.mu.Lock()
	if len(p.buffer) == 0 {
		p.mu.Unlock()
		return nil
	}
	batch := make([]goredis.XAddArgs, len(p.buffer))
	copy(batch, p.buffer)
	p.buffer = p.buffer[:0]
	p.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipe := p.client.Pipeline()
	for i := range batch {
		args := batch[i]
		pipe.XAdd(ctx, &args)
	}
	_, err := pipe.Exec(ctx)
	if err == nil {
		return nil
	}

	// Fallback по одному (дороже, но лучше, чем потерять весь батч).
	// WARNING: при частичном pipeline failure порядок внутри батча не гарантирован.
	// Метрика batch_pipeline_retry_total фиксирует этот риск для SRE-алертинга.
	var first error
	for i := range batch {
		args := batch[i]
		_, e := redisclient.XAddWithRetry(ctx, p.client, &args)
		if e != nil {
			monitoring.RecordBatchPipelineRetry(p.stream, "fail")
			if first == nil {
				first = e
			}
		} else {
			monitoring.RecordBatchPipelineRetry(p.stream, "ok")
		}
	}
	if first != nil {
		return first
	}
	return err
}

// Close останавливает publisher и делает финальный Flush.
func (p *BatchStreamPublisher) Close(timeout time.Duration) error {
	var err error
	p.once.Do(func() {
		close(p.stopCh)

		done := make(chan struct{})
		go func() {
			err = p.Flush()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(timeout):
			// best-effort
		}
	})
	return err
}
