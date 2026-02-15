# `queue` — Типобезопасная очередь заданий для Go

[![Go CI](https://github.com/shuldan/queue/workflows/Go%20CI/badge.svg)](https://github.com/shuldan/queue/actions)
[![codecov](https://codecov.io/gh/shuldan/queue/branch/main/graph/badge.svg)](https://codecov.io/gh/shuldan/queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Пакет `queue` предоставляет типобезопасную, отказоустойчивую очередь заданий для Go-приложений, построенных по принципам DDD. Поддерживает параллельную обработку, retry-стратегии, DLQ, middleware, восстановление после паник и структурированную обработку ошибок. В комплекте — брокеры на основе **in-memory** и **Redis Streams**.

---

## Содержание

- [Возможности](#-возможности)
- [Установка](#-установка)
- [Быстрый старт](#-быстрый-старт)
- [Архитектура](#-архитектура)
  - [Broker](#broker--интерфейс-транспорта)
  - [Queue](#queue--типобезопасная-очередь)
  - [Envelope](#envelope--конверт-сообщения)
  - [Serializer](#serializer--сериализация)
  - [Middleware](#middleware--цепочка-обработки)
  - [Error & Panic Handlers](#errorhandler--panichandler)
  - [Backoff](#backoff--стратегии-задержки)
- [Брокеры](#-брокеры)
  - [Memory](#memory)
  - [Redis Streams](#redis-streams)
- [Конфигурация](#-конфигурация)
  - [Опции очереди](#опции-очереди)
  - [Опции публикации](#опции-публикации)
  - [Опции Redis-брокера](#опции-redis-брокера)
- [Примеры](#-примеры)
  - [Базовый пример с memory-брокером](#1-базовый-пример-с-memory-брокером)
  - [Redis Streams в production](#2-redis-streams-в-production)
  - [Middleware: логирование и метрики](#3-middleware-логирование-и-метрики)
  - [Кастомный ErrorHandler](#4-кастомный-errorhandler)
  - [Работа с заголовками и метаданными](#5-работа-с-заголовками-и-метаданными)
  - [Graceful shutdown](#6-graceful-shutdown)
  - [Кастомный Serializer](#7-кастомный-serializer)
  - [Потребление из DLQ](#8-потребление-из-dlq)
- [Жизненный цикл сообщения](#-жизненный-цикл-сообщения)
- [Важные замечания](#-важные-замечания)
- [Разработка](#-разработка)
- [Лицензия](#-лицензия)

---

## 🚀 Возможности

| Возможность | Описание |
|---|---|
| **Типобезопасность** | Generics — тип задания проверяется при создании очереди |
| **Message Envelope** | Каждое сообщение оборачивается в конверт с ID, заголовками, timestamps |
| **Middleware** | Цепочка middleware для логирования, метрик, трейсинга |
| **Параллельная обработка** | Настраиваемый пул воркеров |
| **Retry с backoff** | Фиксированная, экспоненциальная стратегии или без задержки |
| **Dead Letter Queue** | Отправка в DLQ после исчерпания попыток |
| **Panic recovery** | Паники в хендлерах перехватываются со стектрейсом |
| **Graceful shutdown** | Корректное завершение с дообработкой in-flight заданий |
| **Health check** | `Ping(ctx)` для readiness / liveness проб |
| **Pluggable Serializer** | JSON по умолчанию, заменяется на protobuf, msgpack и др. |
| **Pluggable Broker** | Интерфейс `Broker` — подключайте любой транспорт |
| **Контекст с метаданными** | `MessageMeta` доступен из контекста внутри хендлера и middleware |
| **Разделённый lifecycle** | Очередь и брокер закрываются независимо |

---

## 📦 Установка

Требуется **Go 1.24+**.

```sh
go get github.com/shuldan/queue
```

Для Redis-брокера:

```sh
go get github.com/redis/go-redis/v9
```

---

## ⚡ Быстрый старт

```go
package main

import (
	"context"
	"fmt"

	"github.com/shuldan/queue"
	"github.com/shuldan/queue/broker/memory"
)

type OrderCreated struct {
	OrderID string
	UserID  string
}

func main() {
	// 1. Создаём брокер
	b := memory.New()
	defer b.Close()

	// 2. Создаём очередь
	q, err := queue.New[*OrderCreated](b,
		queue.WithWorkerCount(2),
		queue.WithMaxRetries(3),
		queue.WithDLQ(true),
	)
	if err != nil {
		panic(err)
	}
	defer q.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Запускаем потребителя
	go func() {
		_ = q.Consume(ctx, func(ctx context.Context, job *OrderCreated) error {
			fmt.Printf("Processing order %s for user %s\n", job.OrderID, job.UserID)
			return nil
		})
	}()

	// 4. Публикуем задание
	_ = q.Produce(ctx, &OrderCreated{OrderID: "ORD-1", UserID: "USR-42"})

	// ... приложение работает ...
}
```

---

## 🧱 Архитектура

### `Broker` — интерфейс транспорта

```go
type Broker interface {
    Produce(ctx context.Context, topic string, data []byte) error
    Consume(ctx context.Context, topic string, handler func([]byte) error) error
    Ping(ctx context.Context) error
    Close() error
}
```

Брокер отвечает только за доставку байтов. Сериализация, конверт, retry — задача `Queue`.

> **Важно:** `Queue.Close()` **не** закрывает брокер. Если несколько очередей разделяют один брокер, каждая управляет только своим lifecycle. Брокер закрывается вызывающим кодом отдельно.

### `Queue` — типобезопасная очередь

```go
q, err := queue.New[*MyJob](broker, opts...)
```

- `T` должен быть **указателем на структуру** (`*MyStruct`). Проверяется при создании.
- Имя топика извлекается из типа (`MyStruct`) или задаётся явно через `WithTopic()`.
- Метод `Produce` сериализует задание, оборачивает в `Envelope` и отправляет в брокер.
- Метод `Consume` блокируется до отмены контекста или вызова `Close()`.

### `Envelope` — конверт сообщения

Каждое сообщение автоматически оборачивается в конверт:

```go
type Envelope struct {
    ID        string            // UUID v4
    Topic     string            // имя топика
    Data      []byte            // сериализованное задание
    Headers   map[string]string // пользовательские заголовки
    Attempt   int               // номер попытки (для персистентных retry)
    CreatedAt time.Time         // время создания (UTC)
}
```

Конверт сериализуется в JSON и передаётся брокеру как `[]byte`. При потреблении конверт десериализуется один раз, данные из `Data` десериализуются в тип `T`.

### `Serializer` — сериализация

```go
type Serializer interface {
    Marshal(v any) ([]byte, error)
    Unmarshal(data []byte, v any) error
}
```

По умолчанию — `JSONSerializer`. Заменяется через `WithSerializer()`:

```go
queue.New[*MyJob](broker, queue.WithSerializer(myProtobufSerializer))
```

> Конверт (`Envelope`) всегда сериализуется в JSON независимо от выбранного `Serializer`. Пользовательский сериализатор применяется только к данным задания (`Data`).

### `Middleware` — цепочка обработки

```go
type Middleware[T any] func(next func(context.Context, T) error) func(context.Context, T) error
```

Middleware регистрируется через `Use()` **до** вызова `Consume()`:

```go
q.Use(loggingMiddleware, metricsMiddleware)
```

Middleware выполняется в порядке регистрации (первый зарегистрированный — внешний слой). Внутри middleware доступен контекст с `MessageMeta`:

```go
func loggingMiddleware(next func(context.Context, *MyJob) error) func(context.Context, *MyJob) error {
    return func(ctx context.Context, job *MyJob) error {
        meta, _ := queue.MetaFromContext(ctx)
        slog.Info("processing",
            "message_id", meta.ID,
            "topic", meta.Topic,
            "attempt", meta.Attempt,
        )
        return next(ctx, job)
    }
}
```

### `ErrorHandler` & `PanicHandler`

Обработчики получают **структурированный контекст**, а не `any`:

```go
type ErrorContext struct {
    Topic     string
    MessageID string
    Attempt   int
    Err       error
}

type PanicContext struct {
    Topic      string
    MessageID  string
    PanicValue any
    Stack      []byte
}

type ErrorHandler interface {
    Handle(ctx ErrorContext)
}

type PanicHandler interface {
    Handle(ctx PanicContext)
}
```

По умолчанию ошибки и паники логируются через `slog`. Заменяются через `WithErrorHandler()` и `WithPanicHandler()`.

### `Backoff` — стратегии задержки

```go
type BackoffStrategy interface {
    Delay(attempt int) time.Duration
}
```

| Стратегия | Поведение |
|---|---|
| `FixedBackoff{Duration}` | Одинаковая задержка на каждую попытку |
| `ExponentialBackoff{Base, MaxDelay}` | Удваивается с каждой попыткой до `MaxDelay` |
| `NoBackoff{}` | Без задержки (немедленный retry) |

---

## 📡 Брокеры

### Memory

In-memory брокер на основе буферизированных каналов. Подходит для тестов и лёгких сценариев.

```go
import "github.com/shuldan/queue/broker/memory"

b := memory.New()
defer b.Close()
```

**Особенности:**

- Сообщения хранятся в памяти и не переживают перезапуск процесса.
- При `Close()` буферизированные сообщения дочитываются (best-effort drain).
- `Ping()` проверяет, что брокер не закрыт.
- Безопасен для конкурентного использования.

### Redis Streams

Production-ready брокер на основе Redis Streams с consumer groups.

```go
import (
    goredis "github.com/redis/go-redis/v9"
    redisBroker "github.com/shuldan/queue/broker/redis"
)

client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})

b := redisBroker.New(client,
    redisBroker.WithConsumerGroup("payments"),
    redisBroker.WithProcessingTimeout(60 * time.Second),
    redisBroker.WithMaxStreamLength(100000),
    redisBroker.WithClaim(true),
)
defer b.Close()
```

**Особенности:**

- Автоматическое создание стримов и consumer groups (`XGROUP CREATE ... MKSTREAM`).
- Подтверждение обработки через `XACK`.
- Автоматический `XCLAIM` зависших сообщений (настраиваемый интервал и таймаут).
- Очистка consumer при завершении (`XGROUP DELCONSUMER`).
- `Ping()` делегирует `client.Ping()` — проверяет доступность Redis.
- Корректная обработка ошибок `ERR no such key` и `BUSYGROUP`.
- Backoff при ошибках чтения для предотвращения busy loop.

---

## ⚙️ Конфигурация

### Опции очереди

```go
q, err := queue.New[*MyJob](broker, opts...)
```

| Опция | По умолчанию | Описание |
|---|---|---|
| `WithTopic(name)` | из типа `T` | Явное имя топика |
| `WithPrefix(prefix)` | `""` | Префикс для топика и DLQ |
| `WithWorkerCount(n)` | `runtime.NumCPU()` | Число параллельных воркеров |
| `WithMaxRetries(n)` | `3` | Максимум повторных попыток |
| `WithBackoff(strategy)` | `FixedBackoff{1s}` | Стратегия задержки между retry |
| `WithDLQ(enabled)` | `false` | Автоматическая отправка в DLQ |
| `WithBufferSize(n)` | `100` | Размер внутреннего буфера заданий |
| `WithSerializer(s)` | `JSONSerializer{}` | Сериализатор для данных задания |
| `WithErrorHandler(h)` | slog-логирование | Обработчик ошибок |
| `WithPanicHandler(h)` | slog-логирование | Обработчик паник |

> Все опции валидируются при создании очереди. Передача `nil` для `BackoffStrategy`, `Serializer`, `ErrorHandler` или `PanicHandler` приведёт к ошибке.

### Опции публикации

```go
q.Produce(ctx, job, opts...)
```

| Опция | Описание |
|---|---|
| `WithHeaders(map[string]string)` | Заголовки, прикрепляемые к конверту сообщения |

### Опции Redis-брокера

```go
redisBroker.New(client, opts...)
```

| Опция | По умолчанию | Описание |
|---|---|---|
| `WithStreamKeyFormat(fmt)` | `"stream:%s"` | Формат ключа стрима (содержит `%s` для топика) |
| `WithConsumerGroup(name)` | `"consumers"` | Префикс имени consumer group |
| `WithProcessingTimeout(d)` | `30s` | Время, после которого сообщение считается зависшим |
| `WithClaimInterval(d)` | `1s` | Интервал проверки зависших сообщений |
| `WithMaxClaimBatch(n)` | `10` | Максимум сообщений за один claim |
| `WithBlockTimeout(d)` | `500ms` | Таймаут блокирующего чтения `XREADGROUP` |
| `WithMaxStreamLength(n)` | `0` (без ограничения) | `MAXLEN` для обрезки стрима |
| `WithApproximateTrimming(b)` | `true` | Использовать `~` при обрезке |
| `WithClaim(enabled)` | `true` | Включить автоматический claim |
| `WithConsumerPrefix(prefix)` | `""` | Префикс имени consumer |

---

## 📖 Примеры

### 1. Базовый пример с memory-брокером

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/shuldan/queue"
	"github.com/shuldan/queue/broker/memory"
)

type EmailJob struct {
	To      string
	Subject string
}

func main() {
	b := memory.New()
	defer b.Close()

	q, err := queue.New[*EmailJob](b,
		queue.WithWorkerCount(2),
		queue.WithMaxRetries(3),
		queue.WithBackoff(queue.FixedBackoff{Duration: time.Second}),
		queue.WithDLQ(true),
	)
	if err != nil {
		panic(err)
	}
	defer q.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = q.Consume(ctx, func(ctx context.Context, job *EmailJob) error {
			if job.To == "fail@example.com" {
				return errors.New("temporary failure")
			}
			fmt.Printf("Sent email to %s: %s\n", job.To, job.Subject)
			return nil
		})
	}()

	_ = q.Produce(ctx, &EmailJob{To: "user@example.com", Subject: "Welcome!"})
	_ = q.Produce(ctx, &EmailJob{To: "fail@example.com", Subject: "Retry me"})

	time.Sleep(5 * time.Second)
}
```

### 2. Redis Streams в production

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/shuldan/queue"
	redisBroker "github.com/shuldan/queue/broker/redis"
)

type PaymentProcessed struct {
	OrderID string
	Amount  float64
}

func main() {
	client := goredis.NewClient(&goredis.Options{
		Addr: "localhost:6379",
	})

	b := redisBroker.New(client,
		redisBroker.WithConsumerGroup("payments"),
		redisBroker.WithProcessingTimeout(60*time.Second),
		redisBroker.WithMaxStreamLength(100000),
		redisBroker.WithClaim(true),
	)
	defer b.Close()

	q, err := queue.New[*PaymentProcessed](b,
		queue.WithWorkerCount(8),
		queue.WithMaxRetries(10),
		queue.WithBackoff(queue.ExponentialBackoff{
			Base:     500 * time.Millisecond,
			MaxDelay: 30 * time.Second,
		}),
		queue.WithDLQ(true),
		queue.WithPrefix("prod:"),
	)
	if err != nil {
		panic(err)
	}
	defer q.Close()

	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM,
	)
	defer stop()

	slog.Info("starting consumer",
		"topic", q.Topic(),
		"dlq", q.DLQTopic(),
	)

	err = q.Consume(ctx, func(ctx context.Context, job *PaymentProcessed) error {
		slog.Info("processing payment",
			"order_id", job.OrderID,
			"amount", job.Amount,
		)
		return nil
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("consumer stopped with error", "error", err)
	}
}
```

### 3. Middleware: логирование и метрики

```go
func loggingMiddleware(
	next func(context.Context, *OrderCreated) error,
) func(context.Context, *OrderCreated) error {
	return func(ctx context.Context, job *OrderCreated) error {
		meta, _ := queue.MetaFromContext(ctx)
		start := time.Now()

		slog.Info("job started",
			"message_id", meta.ID,
			"topic", meta.Topic,
			"attempt", meta.Attempt,
		)

		err := next(ctx, job)

		slog.Info("job finished",
			"message_id", meta.ID,
			"duration", time.Since(start),
			"error", err,
		)

		return err
	}
}

func metricsMiddleware(
	next func(context.Context, *OrderCreated) error,
) func(context.Context, *OrderCreated) error {
	return func(ctx context.Context, job *OrderCreated) error {
		start := time.Now()
		err := next(ctx, job)
		duration := time.Since(start)

		// prometheus.JobDuration.Observe(duration.Seconds())
		// if err != nil { prometheus.JobErrors.Inc() }
		_ = duration

		return err
	}
}

// Регистрация middleware — до вызова Consume:
q.Use(loggingMiddleware, metricsMiddleware)
```

### 4. Кастомный ErrorHandler

```go
type alertingErrorHandler struct {
	alerter Alerter
}

func (h *alertingErrorHandler) Handle(ctx queue.ErrorContext) {
	slog.Error("job failed",
		"topic", ctx.Topic,
		"message_id", ctx.MessageID,
		"attempt", ctx.Attempt,
		"error", ctx.Err,
	)

	// Алерт после N попыток
	if ctx.Attempt >= 5 {
		h.alerter.Send(fmt.Sprintf(
			"Job %s failed %d times: %v",
			ctx.MessageID, ctx.Attempt, ctx.Err,
		))
	}
}

// Использование:
q, _ := queue.New[*MyJob](broker,
	queue.WithErrorHandler(&alertingErrorHandler{alerter: myAlerter}),
)
```

### 5. Работа с заголовками и метаданными

```go
// Публикация с заголовками:
err := q.Produce(ctx, &OrderCreated{OrderID: "123"},
	queue.WithHeaders(map[string]string{
		"x-correlation-id": "req-abc-def",
		"x-source":         "api-gateway",
	}),
)

// Чтение заголовков в хендлере или middleware:
func handler(ctx context.Context, job *OrderCreated) error {
	meta, ok := queue.MetaFromContext(ctx)
	if !ok {
		return errors.New("no message metadata")
	}

	correlationID := meta.Headers["x-correlation-id"]
	slog.Info("processing",
		"order_id", job.OrderID,
		"correlation_id", correlationID,
		"created_at", meta.CreatedAt,
	)

	return nil
}
```

### 6. Graceful shutdown

```go
func main() {
	b := memory.New()

	q, _ := queue.New[*MyJob](b,
		queue.WithWorkerCount(4),
	)

	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM,
	)
	defer stop()

	// Consume блокируется до отмены контекста
	go func() {
		if err := q.Consume(ctx, handler); err != nil {
			slog.Error("consume error", "error", err)
		}
	}()

	<-ctx.Done()

	slog.Info("shutting down...")

	// Close() отменяет внутренний контекст очереди
	// и ждёт завершения всех in-flight заданий.
	// Брокер НЕ закрывается — это ответственность вызывающего кода.
	if err := q.Close(); err != nil {
		slog.Error("queue close error", "error", err)
	}

	if err := b.Close(); err != nil {
		slog.Error("broker close error", "error", err)
	}

	slog.Info("shutdown complete")
}
```

### 7. Кастомный Serializer

```go
import "google.golang.org/protobuf/proto"

type ProtobufSerializer struct{}

func (ProtobufSerializer) Marshal(v any) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, errors.New("value must implement proto.Message")
	}
	return proto.Marshal(msg)
}

func (ProtobufSerializer) Unmarshal(data []byte, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return errors.New("value must implement proto.Message")
	}
	return proto.Unmarshal(data, msg)
}

// Использование:
q, _ := queue.New[*MyProtoJob](broker,
	queue.WithSerializer(ProtobufSerializer{}),
)
```

> Конверт (`Envelope`) всегда кодируется в JSON. Пользовательский `Serializer` применяется только к полю `Data` внутри конверта.

### 8. Потребление из DLQ

DLQ — это обычный топик. Создайте отдельную очередь с тем же типом и `WithTopic()`:

```go
// Основная очередь
q, _ := queue.New[*OrderCreated](broker,
	queue.WithDLQ(true),
	queue.WithPrefix("prod:"),
)

// Очередь для обработки DLQ
dlq, _ := queue.New[*OrderCreated](broker,
	queue.WithTopic(q.DLQTopic()), // "prod:dlq:OrderCreated"
	queue.WithMaxRetries(0),       // не retry'ить повторно
)

go func() {
	_ = dlq.Consume(ctx, func(ctx context.Context, job *OrderCreated) error {
		meta, _ := queue.MetaFromContext(ctx)
		slog.Warn("DLQ job",
			"message_id", meta.ID,
			"order_id", job.OrderID,
		)
		// Сохранить в БД, отправить алерт, переопубликовать и т.д.
		return nil
	})
}()
```

---

## 🔄 Жизненный цикл сообщения

```
Produce(job)
    │
    ▼
Serialize(job) ──► Envelope{ID, Data, Headers, CreatedAt}
    │
    ▼
marshalEnvelope ──► []byte
    │
    ▼
broker.Produce(topic, []byte)
    │
    ▼
═══════════ transport (memory / Redis Streams) ═══════════
    │
    ▼
broker.Consume callback ──► jobs channel
    │
    ▼
Worker pool (N goroutines)
    │
    ▼
unmarshalEnvelope ──► Envelope
    │
    ▼
serializer.Unmarshal(env.Data) ──► T (typed job)
    │
    ▼
WithMeta(ctx, MessageMeta) ──► ctx с метаданными
    │
    ▼
Middleware chain ──► handler(ctx, job)
    │
    ├─ nil ──► success
    │
    └─ error ──► retry (attempt < maxRetries)
                    │
                    ├─ backoff.Delay(attempt)
                    │
                    └─ исчерпаны ──► DLQ (если включён)
```

---

## ⚠️ Важные замечания

### Семантика доставки

Текущая реализация обеспечивает **at-most-once** семантику для in-memory брокера и **at-least-once** для Redis-брокера (с `XACK` и `XCLAIM`). Идемпотентность хендлера — ответственность разработчика.

### Разделённый lifecycle

```go
b := memory.New()       // вызывающий код владеет брокером
q1, _ := queue.New[*JobA](b)
q2, _ := queue.New[*JobB](b)

// q1.Close() НЕ закрывает b — q2 продолжает работать
q1.Close()

// Брокер закрывается отдельно, после всех очередей
q2.Close()
b.Close()
```

### Именование топиков

- По умолчанию топик извлекается из имени типа: `*mypkg.OrderCreated` → `"OrderCreated"`.
- При рефакторинге (переименование структуры) топик изменится. Для предсказуемости используйте `WithTopic()`.
- С `WithPrefix("prod:")` топик становится `"prod:OrderCreated"`, DLQ — `"prod:dlq:OrderCreated"`.

### Retry

- Retry выполняется in-process в рамках текущего воркера.
- Счётчик попыток сохраняется в `Envelope.Attempt`.
- При краше процесса и повторном `XCLAIM` Redis-сообщения retry начинается с `Attempt`, сохранённого в конверте.

### Thread safety

- `Produce`, `Close`, `Ping` безопасны для конкурентных вызовов.
- `Use()` (регистрация middleware) должен вызываться **до** `Consume()`.
- Несколько вызовов `Consume()` на одной очереди создают независимые пулы воркеров, конкурирующие за одни и те же сообщения.

### Валидация

При создании очереди проверяется:
- `T` — указатель на структуру.
- `BackoffStrategy`, `Serializer`, `ErrorHandler`, `PanicHandler` — не `nil`.
- `WorkerCount`, `BufferSize` — не менее 1.

---

## 🛠️ Разработка

### Установка инструментов

```sh
go install golang.org/x/tools/cmd/goimports@latest
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v2.4.0
go install github.com/securego/gosec/v2/cmd/gosec@latest
```

### Запуск проверок

```sh
# Форматирование
gofmt -s -l .
goimports -local github.com/shuldan/queue -l .

# Линтер
golangci-lint run --config .golangci-lint.yaml

# Безопасность
gosec -exclude-dir=test ./...

# Тесты с покрытием (≥70%)
go test -race -count=1 -timeout=30s -coverprofile=coverage.out -covermode=atomic ./...
go tool cover -func=coverage.out
```

### CI

Проверки запускаются автоматически через GitHub Actions (`.github/workflows/go.yml`):
- форматирование (`gofmt`, `goimports`),
- линтер (`golangci-lint`),
- безопасность (`gosec`),
- тесты с покрытием ≥70%.

---

## 📝 Лицензия

Распространяется под лицензией [MIT](LICENSE).

---

## 🤝 Вклад в проект

PR и issue приветствуются. Перед отправкой:

1. Запустите все проверки (`gofmt`, `golangci-lint`, `gosec`, тесты).
2. Покройте новый функционал тестами.
3. Обновите документацию при изменении API.

---

> **Репозиторий**: [github.com/shuldan/queue](https://github.com/shuldan/queue)
> **Go**: 1.24+
