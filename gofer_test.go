package gofer

import (
	"log"
	"testing"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

func GetTestConfig(t *testing.T) *ConnParam {
	viper.AutomaticEnv()
	temp := viper.Get("rabbit_test_host")
	log.Println(temp)
	host, ok := viper.Get("rabbit_test_host").(string)
	if !ok {
		t.Fatalf("RABBIT_TEST_HOST envvar not set. Please ensure the env var RABBIT_TEST_HOST is set, and points to an accessible rabbit host.")
	}
	port, ok := viper.Get("rabbit_test_port").(int)
	if !ok || port == 0 {
		port = 5672
	}
	user := "guest"
	secret := "guest"
	return &ConnParam{
		Host:   host,
		Port:   port,
		Secret: secret,
		UserId: user,
	}
}

type TestInfo struct {
	Elements []int
}

func TestGofer(t *testing.T) {
	var (
		params     = GetTestConfig(t)
		testType   = "testType"
		testFlavor = "testFlavor"
	)

	g, err := New("rabbitmq", params, "gofer-test", 1)
	if err != nil {
		t.Fatal(err)
	}
	extra := map[string]interface{}{"this": "that", "float": 153.2, "bool": true}
	err = g.QueueTask(&Task{
		Type:     testType,
		Flavor:   testFlavor,
		Retry:    false,
		Priority: 1,
		Extra:    extra,
	}, 0)
	if err != nil {
		t.Fatalf("Couldn't queue the task %v", err)
	}
	time.Sleep(time.Millisecond * time.Duration(10)) // wait for queue to update count
	count, _ := g.CountForType(testType)
	if count != 1 {
		t.Fatalf("Incorrect task queue count; got %d expected %d", count, 1)
	}
	tasks, err := g.Tasks(testType, true)
	for task := range tasks {
		task.Ack()
		if task.Type != testType {
			t.Fatal("wrong type")
		} else {
			var out map[string]interface{}
			mapstructure.Decode(task.Extra, &out)
			for k, v := range out {
				if v != extra[k] {
					t.Fatalf("%v doesn't match %v: %v", k, v, extra[k])
				}
			}
		}
		err = g.QueueTask(&task, 0)
		if err == nil || err.Error() != ErrRetryExceeded {
			t.Fatal("requeued task when it shoudn't have been.")
		}
		break
	}
}
