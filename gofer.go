/*
Package gofer is a distributed task queue for go. Clients of the package can post Task requests,
and register to consume Tasks of a given type. It uses gq to pass Tasks to
a message broker (e.g. RabbitMQ, AWS SQS), routing them based on the task's type.
Future versions may allow subscribing to flavors of a given type.
*/
package gofer

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/gpitfield/gq"
	_ "github.com/gpitfield/gq/lib/rabbitmq"
)

const ErrRetryExceeded = "Task request retry count exceeded; did not queue."

type Gofer struct {
	*gq.Service
	QueuePrefix string
	MaxRetries  int
}

type ConnParam gq.ConnParam

// Return a new gofer
func New(broker string, config *ConnParam, qPrefix string, retries int) (*Gofer, error) {
	svc := gq.New()
	gqp := gq.ConnParam(*config)
	err := svc.Open(broker, &gqp)
	if err != nil {
		return nil, err
	}
	gofer := &Gofer{
		Service:     svc,
		QueuePrefix: qPrefix,
		MaxRetries:  retries,
	}
	return gofer, nil
}

/*
Task includes information necessary to request different types of tasks.
The Extra field can be used to embed structs, which can in turn be marshalled into their original
type using github.com/mitchellh/mapstructure or similar.
*/
type Task struct {
	Type       string      // the type of task being requested
	Flavor     string      // the flavor of the task
	Retry      bool        // should we retry if this fails
	RetryCount int         // number of times we have retried
	Priority   int         // 0-100; higher is more important
	Extra      interface{} // must be json-encodable
	ack        func() error
}

func (t *Task) Ack() error {
	if t.ack != nil {
		return t.ack()
	}
	return nil
}

// Returns the queue name for a given scrape type.
func (g *Gofer) QueueForType(t string) string {
	return g.QueuePrefix + ":" + t
}

// Queue a task.
func (g *Gofer) QueueTask(task *Task, delay time.Duration) (err error) {
	task.RetryCount += 1
	if task.RetryCount > g.MaxRetries {
		return errors.New(ErrRetryExceeded)
	} else {
		bytes, err := json.Marshal(*task)
		if err != nil {
			return err
		}
		queue := g.QueueForType(task.Type)
		return g.PostMessage(queue, gq.Message{Body: bytes, Priority: task.Priority}, delay)
	}
}

// Return a channel of tasks of a given type.
// If ack is true, broker will expect ack on completion of task if supported.
func (g *Gofer) Tasks(taskType string, ack bool) (c chan Task, err error) {
	msgs, err := g.Consume(g.QueueForType(taskType), !ack)
	if err != nil {
		return
	}
	c = make(chan Task)
	go func(messages <-chan gq.Message) {
		for msg := range messages { // taskify the message's body
			var task Task
			err = json.Unmarshal(msg.Body, &task)
			if err != nil {
				log.Println("Error parsing task message", err)
				continue
			}
			if ack && msg.AckFunc != nil {
				task.ack = msg.AckFunc
			}
			c <- task
		}

	}(msgs)
	return
}
