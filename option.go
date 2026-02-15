package queue

import (
	"runtime"
	"time"
)

type Option func(*options)

type options struct {
	errorHandler ErrorHandler
	panicHandler PanicHandler
	workerCount  int
	maxRetries   int
	backoff      BackoffStrategy
	prefix       string
	topic        string
	dlqEnabled   bool
	bufferSize   int
	serializer   Serializer
}

func defaultOptions() *options {
	return &options{
		panicHandler: &defaultPanicHandler{},
		errorHandler: &defaultErrorHandler{},
		workerCount:  runtime.NumCPU(),
		maxRetries:   3,
		backoff:      FixedBackoff{Duration: time.Second},
		bufferSize:   100,
		serializer:   JSONSerializer{},
	}
}

func validateOptions(o *options) error {
	if o.backoff == nil {
		return ErrNilBackoff
	}

	if o.errorHandler == nil {
		return ErrNilErrorHandler
	}

	if o.panicHandler == nil {
		return ErrNilPanicHandler
	}

	if o.serializer == nil {
		return ErrNilSerializer
	}

	return nil
}

func WithTopic(topic string) Option {
	return func(o *options) { o.topic = topic }
}

func WithPrefix(prefix string) Option {
	return func(o *options) { o.prefix = prefix }
}

func WithDLQ(enabled bool) Option {
	return func(o *options) { o.dlqEnabled = enabled }
}

func WithWorkerCount(n int) Option {
	return func(o *options) {
		if n < 1 {
			n = 1
		}
		o.workerCount = n
	}
}

func WithMaxRetries(n int) Option {
	return func(o *options) {
		if n < 0 {
			n = 0
		}
		o.maxRetries = n
	}
}

func WithBackoff(b BackoffStrategy) Option {
	return func(o *options) { o.backoff = b }
}

func WithErrorHandler(h ErrorHandler) Option {
	return func(o *options) { o.errorHandler = h }
}

func WithPanicHandler(h PanicHandler) Option {
	return func(o *options) { o.panicHandler = h }
}

func WithBufferSize(size int) Option {
	return func(o *options) {
		if size < 1 {
			size = 1
		}
		o.bufferSize = size
	}
}

func WithSerializer(s Serializer) Option {
	return func(o *options) { o.serializer = s }
}

type ProduceOption func(*produceOptions)

type produceOptions struct {
	headers map[string]string
}

func WithHeaders(h map[string]string) ProduceOption {
	return func(o *produceOptions) { o.headers = h }
}
