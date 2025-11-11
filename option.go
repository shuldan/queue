package queue

type Option func(*options)

type options struct {
	errorHandler ErrorHandler
	panicHandler PanicHandler
	workerCount  int
	maxRetries   int
	backoff      BackoffStrategy
	prefix       string
	dlqEnabled   bool
}

func WithPrefix(prefix string) Option {
	return func(q *options) {
		q.prefix = prefix
	}
}

func WithDLQ(enabled bool) Option {
	return func(q *options) {
		q.dlqEnabled = enabled
	}
}

func WithWorkerCount(n int) Option {
	return func(q *options) {
		if n < 1 {
			n = 1
		}
		q.workerCount = n
	}
}

func WithMaxRetries(n int) Option {
	return func(q *options) {
		q.maxRetries = n
	}
}

func WithBackoff(b BackoffStrategy) Option {
	return func(q *options) {
		q.backoff = b
	}
}

func WithErrorHandler(h ErrorHandler) Option {
	return func(q *options) {
		q.errorHandler = h
	}
}

func WithPanicHandler(h PanicHandler) Option {
	return func(q *options) {
		q.panicHandler = h
	}
}
