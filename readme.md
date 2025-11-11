Конечно! Вот обновлённый `README.md`, учитывающий добавленные брокеры (`memory` и `redis`), а также уточняющий архитектуру и примеры использования.

---

# `queue` — Гибкая и отказоустойчивая очередь заданий для Go

[![Go CI](https://github.com/shuldan/queue/workflows/Go%20CI/badge.svg)](https://github.com/shuldan/queue/actions)
[![codecov](https://codecov.io/gh/shuldan/queue/branch/main/graph/badge.svg)](https://codecov.io/gh/shuldan/queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Пакет `queue` предоставляет типобезопасную, отказоустойчивую и гибкую очередь заданий (`job queue`) на основе публикации-подписки (`pub/sub`). Поддерживает параллельную обработку, стратегии повторных попыток, DLQ, восстановление после паник и кастомную обработку ошибок. В комплекте поставляются готовые реализации брокеров на основе **in-memory** и **Redis Streams**, а также интерфейс для интеграции с любой системой обмена сообщениями.

---

## 🚀 Основные возможности

- **Типобезопасная очередь**: строгая проверка типа задания при создании (`*Struct`).
- **Параллельная обработка**: настраиваемое число воркеров (`workerCount`).
- **Повторные попытки**: поддержка фиксированной и экспоненциальной стратегий отложки.
- **Dead Letter Queue (DLQ)**: автоматический переход в DLQ при исчерпании попыток.
- **Восстановление после паник**: обработка паник в обработчиках заданий.
- **Контекстная отмена**: корректная реакция на отмену контекста.
- **Префикс топика и DLQ**: поддержка изоляции очередей по префиксам.
- **Кастомные обработчики ошибок и паник**: через интерфейсы `ErrorHandler` и `PanicHandler`.
- **Готовые брокеры**:
    - `memory`: встроенный in-memory брокер для тестов и лёгких сценариев.
    - `redis`: production-ready брокер на Redis Streams с поддержкой групп потребителей, claim’ов, ACK и автоматического управления потоками.

---

## 📦 Установка

Требуется **Go 1.24+**.

```sh
go get github.com/shuldan/queue
```

Для использования Redis-брокера убедитесь, что зависимости установлены:

```sh
go get github.com/redis/go-redis/v9
```

---

## 🛠️ Работа с проектом

### Установка инструментов

```sh
make install-tools
```

Устанавливает:
- `golangci-lint` (v2.4.0)
- `goimports`
- `gosec`

### Локальная проверка

```sh
make all
```

Выполняет:
- проверку форматирования (`gofmt`, `goimports`),
- статический анализ (`golangci-lint`),
- security-сканирование (`gosec`),
- запуск всех тестов.

### CI-проверка

```sh
make ci
```

Аналогично тому, что запускается в GitHub Actions, включая покрытие тестами (требуется ≥70%).

---

## 🧱 Архитектура

### `Broker` — интерфейс брокера

```go
type Broker interface {
	Produce(ctx context.Context, topic string, data []byte) error
	Consume(ctx context.Context, topic string, handler func([]byte) error) error
	Close() error
}
```

В комплекте идут две реализации:

#### 1. In-Memory брокер

Идеален для тестов и простых сценариев:

```go
import "github.com/shuldan/queue/broker/memory"

broker := memory.New()
```

#### 2. Redis Streams брокер

Production-ready решение с поддержкой отказоустойчивости:

```go
import (
	"github.com/redis/go-redis/v9"
	"github.com/shuldan/queue/broker/redis"
)

client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
broker := redis.New(client,
	redis.WithConsumerGroup("myapp"),
	redis.WithProcessingTimeout(30 * time.Second),
	redis.WithMaxStreamLength(10000),
)
```

См. все опции в разделе **Брокеры** ниже.

### Создание очереди

Задание **должно быть указателем на структуру**:

```go
type OrderProcessed struct {
	ID     string
	Status string
}

q, err := queue.New[*OrderProcessed](broker,
	queue.WithPrefix("prod:"),
	queue.WithWorkerCount(4),
	queue.WithMaxRetries(5),
	queue.WithBackoff(queue.ExponentialBackoff{
		Base:     time.Second,
		MaxDelay: 30 * time.Second,
	}),
	queue.WithDLQ(true),
)
```

### Публикация задания

```go
err := q.Produce(ctx, &OrderProcessed{ID: "123", Status: "completed"})
```

### Обработка заданий

```go
err := q.Consume(ctx, func(ctx context.Context, job *OrderProcessed) error {
	// Обработка задания
	return nil // или ошибка → будет повтор
})
```

---

## 🧪 Примеры использования

### 1. In-Memory (для тестов)

```go
package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/shuldan/queue"
	"github.com/shuldan/queue/broker/memory"
)

type EmailJob struct {
	To      string
	Subject string
}

func main() {
	broker := memory.New()
	q, err := queue.New[*EmailJob](broker,
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
			slog.Info("Sending email", "to", job.To, "subject", job.Subject)
			if job.To == "fail@example.com" {
				return errors.New("temporary failure")
			}
			return nil
		})
	}()

	_ = q.Produce(ctx, &EmailJob{To: "user@example.com", Subject: "Welcome!"})
	_ = q.Produce(ctx, &EmailJob{To: "fail@example.com", Subject: "Retry me!"})

	time.Sleep(5 * time.Second)
}
```

### 2. Redis Streams (production)

```go
package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/shuldan/queue"
	"github.com/shuldan/queue/broker/redis"
)

type PaymentProcessed struct {
	OrderID string
	Amount  float64
}

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	broker := redis.New(client,
		redis.WithConsumerGroup("payments"),
		redis.WithProcessingTimeout(60*time.Second),
		redis.WithMaxStreamLength(100000),
		redis.WithClaim(true),
	)

	q, err := queue.New[*PaymentProcessed](broker,
		queue.WithWorkerCount(8),
		queue.WithMaxRetries(10),
		queue.WithBackoff(queue.ExponentialBackoff{
			Base:     500 * time.Millisecond,
			MaxDelay: 30 * time.Second,
		}),
		queue.WithDLQ(true),
	)
	if err != nil {
		panic(err)
	}
	defer q.Close()

	ctx := context.Background()
	err = q.Consume(ctx, func(ctx context.Context, job *PaymentProcessed) error {
		slog.Info("Processing payment", "order", job.OrderID, "amount", job.Amount)
		// ...
		return nil
	})
	if err != nil {
		panic(err)
	}
}
```

---

## 📦 Брокеры

### `memory` — In-Memory брокер

- Простой и легковесный.
- Поддерживает несколько топиков и конкурентных потребителей.
- **Не предназначен для production**: данные не сохраняются при перезапуске.

### `redis` — Redis Streams брокер

- Использует Redis Streams + Consumer Groups.
- Поддержка:
    - автоматического создания потоков и групп потребителей,
    - `XACK` для подтверждения обработки,
    - `XCLAIM` для повторной обработки "зависших" сообщений,
    - ограничения длины потока (`XADD MAXLEN`),
    - кастомного формата ключей и префиксов потребителей.

#### Опции брокера Redis

```go
redis.New(client,
redis.WithStreamKeyFormat("app:queue:%s"),
redis.WithConsumerGroup("workers"),
redis.WithProcessingTimeout(30 * time.Second),
redis.WithClaimInterval(1 * time.Second),
redis.WithMaxClaimBatch(20),
redis.WithBlockTimeout(500 * time.Millisecond),
redis.WithMaxStreamLength(10000),
redis.WithApproximateTrimming(true),
redis.WithClaim(true),
redis.WithConsumerPrefix("svc-payment"),
)
```

---

## 📄 Лицензия

Распространяется под лицензией [MIT](LICENSE).

---

## 🤝 Вклад в проект

PR и issue приветствуются! Обязательно соблюдайте стиль кода, покрывайте новый функционал тестами и запускайте `make all` перед отправкой.

---

> **Автор**: MSeytumerov  
> **Репозиторий**: `github.com/shuldan/queue`  
> **Go**: `1.24.2`