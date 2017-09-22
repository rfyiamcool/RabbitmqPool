package mq

import (
	"conf"
	"fmt"
	logger "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
	"utils"
)

const (
	TIMES               = 3   // 默认重试次数
	CONNECT_RETRY_DELEY = 500 // 连接重试延迟时间
)

var (
	mq_logger     = logger.WithFields(logger.Fields{"module": "rabbitmq"})
	RControlGroup = utils.NewControlGroup()
)

func failOnError(err error, msg string) {
	if err != nil {
		logger.Errorf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func GetConn(cc map[string]string) RabbitmqConfig {
	return RabbitmqConfig{
		Uri:        cc["Uri"],
		ExName:     cc["ExName"],
		ExType:     cc["ExType"],
		QueueName:  cc["RoutineKey"],
		RoutingKey: cc["RoutingKey"],
	}
}

type RabbitmqConfig struct {
	Uri        string
	ExName     string
	ExType     string
	QueueName  string
	RoutingKey string
	TryTimes   int
	Conn       *amqp.Connection
	Ch         *amqp.Channel
}

func (r *RabbitmqConfig) GetQueueLength() int {
	r.InitConn()
	q, _ := r.Ch.QueueDeclare(
		r.QueueName, // name
		true,        // durable
		false,       // delete when usused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	return q.Messages
}

func (r *RabbitmqConfig) InitConn() error {
	if r.Conn != nil {
		return nil
	}

	conn, err := amqp.Dial(r.Uri)
	if err != nil {
		logger.Errorf("%s", err)
		return conf.ErrRabbimqConnNotConnect
	}
	r.Conn = conn // return ptr

	// add
	ch, err := r.Conn.Channel()
	failOnError(err, "Failed to open a channel")

	err = ch.ExchangeDeclare(
		r.ExName, // name
		r.ExType, // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	r.Ch = ch

	q, err_q := ch.QueueDeclare(
		r.QueueName, // name
		true,        // durable
		false,       // delete when usused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err_q, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,       // queue name
		r.RoutingKey, // routing key
		r.ExName,     // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")
	// end

	return nil
}

func (r *RabbitmqConfig) CloseConn() {
	if r.Conn != nil {
		r.Conn.Close()
		r.Ch.Close()
		// 避免其他人触发Close时，异常 !
		r.Conn = nil
		r.Ch = nil
	}
}

// 后期加入 content, 优雅退出PTR.

func (r *RabbitmqConfig) PushConnTryCatch(body string, callback func(string)) error {
	var err error
	for {
		err = r.Push(body, callback)
		if err != nil {
			mq_logger.Error(err)
			r.CloseConn()
			time.Sleep(time.Duration(conf.MQ_TRY_CONN_DELAY) * time.Millisecond)
			continue
		}
		break
	}
	return err
}

func (r *RabbitmqConfig) PullConnTryCatch(callback func(string)) error {
	var err error
	for {
		err = r.Pull(callback)
		if err != nil {
			mq_logger.Error(err)
			r.CloseConn()
			time.Sleep(time.Duration(conf.MQ_TRY_CONN_DELAY) * time.Millisecond)
			continue
		}
		break
	}
	return err
}

/*

	需要传递body及回调函数.

*/
func (r *RabbitmqConfig) Push(body string, f func(string)) error {
	if err := r.InitConn(); err != nil {
		r.CloseConn()
		return err
	}
	// defer r.Conn.Close()

	err := r.Ch.Publish(
		r.ExName,     // exchange
		r.RoutingKey, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(body),
			DeliveryMode: 2,
		})

	if err != nil {
		logger.Errorf("Failed to publish a message, %s", err)
		return err
	}
	f(body)
	return nil
}

func (r *RabbitmqConfig) ResetConn() error {
	r.Conn.Close()
	r.Ch.Close()
	return nil
}

/*

	1. 需要绑定RoutingKey -- Queue的关系.
	2. 尽量要实现RoutingKey与queue的同名.
	3. ...

*/
func (r *RabbitmqConfig) Pull(f func(string)) error {
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		r.CloseConn()
	// 	}
	// }()

	if err := r.InitConn(); err != nil {
		r.CloseConn()
		return err
	}

	msgs, err := r.Ch.Consume(
		r.QueueName, // queue
		"only_user", // consumer
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)

	// failOnError(err, "Failed to register a consumer")
	if err != nil {
		logger.Errorf("Failed to register a consumer, %s", err)
		return conf.ErrCodeRabbimqConnNotOpen
	}

	// for {
	// 	d, ok := <-msgs
	// 	if !ok {
	// 		time.Second(100 * time.Millisecond)
	// 		continue
	// 	}
	// 	f(string(d.Body))
	// }

	// 有严重的性能问题, 因为不断的初始化timer chan
	timer := time.NewTimer(3 * time.Second)

	for RControlGroup.CheckRunning() {
		timer.Reset(3 * time.Second)

		select {
		case d := <-msgs:
			f(string(d.Body))

		case <-timer.C:
			continue
		}
	}

	timer.Stop()
	return nil
}

