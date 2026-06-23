package observability

import "sync/atomic"

// Counter cuenta mensajes procesados con exito. Compartido consumer -> health
// (espejo de MessagesProcessedCounter del baseline). Thread-safe.
type Counter struct{ n atomic.Int64 }

func NewCounter() *Counter { return &Counter{} }

func (c *Counter) Inc()       { c.n.Add(1) }
func (c *Counter) Get() int64 { return c.n.Load() }
