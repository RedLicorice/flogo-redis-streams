package xread

import (
	"fmt"
	//"strings"
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/project-flogo/core/data/coerce"
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/log"
	"github.com/project-flogo/core/trigger"
	redisConn "github.com/redlicorice/flogo-redis-streams/connection/redis"
)

var ctx = context.TODO()

var triggerMd = trigger.NewMetadata(&Settings{}, &HandlerSettings{}, &Output{})

func init() {
	_ = trigger.Register(&Trigger{}, &Factory{})
}

// Factory is a redis trigger factory
type Factory struct {
}

// Metadata implements trigger.Factory.Metadata
func (*Factory) Metadata() *trigger.Metadata {
	return triggerMd
}

// New implements trigger.Factory.New
func (*Factory) New(config *trigger.Config) (trigger.Trigger, error) {
	s := &Settings{}
	err := metadata.MapToStruct(config.Settings, s, true)
	if err != nil {
		return nil, err
	}
	conn, err := coerce.ToConnection(s.Connection)
	if err != nil {
		return nil, err
	}
	return &Trigger{conn: conn.GetConnection().(redisConn.RedisConnection)}, nil
}

// Trigger is a redis trigger
type Trigger struct {
	conn        redisConn.RedisConnection
	channels    []string
	subHandlers []*Handler
}

// Handler is a redis xread handler
type Handler struct {
	shutdown chan bool
	logger   log.Logger
	handler  trigger.Handler
	streams  []string
	conn     redis.Client
}

// Initialize initializes the trigger
func (t *Trigger) Initialize(ctx trigger.InitContext) error {

	var err error

	for _, handler := range ctx.GetHandlers() {
		redisHandler, err := NewXReadHandler(ctx.Logger(), handler, t.conn.Client().(redis.Client))
		if err != nil {
			return err
		}
		t.subHandlers = append(t.subHandlers, redisHandler)
	}

	return err
}

// NewRedisHandler creates a new redis handler to handle a topic
func NewXReadHandler(logger log.Logger, handler trigger.Handler, conn redis.Client) (*Handler, error) {

	redisHandler := &Handler{logger: logger, shutdown: make(chan bool), handler: handler}

	handlerSetting := &HandlerSettings{}
	err := metadata.MapToStruct(handler.Settings(), handlerSetting, true)
	if err != nil {
		return nil, err
	}

	if handlerSetting.Streams == "" {
		return nil, fmt.Errorf("No stream name was provided for handler: [%s]", handler)
	}

	redisHandler.streams = strings.Split(handlerSetting.Streams, ";")
	redisHandler.conn = conn
	return redisHandler, nil
}

// Start starts the kafka trigger
func (t *Trigger) Start() error {

	for _, handler := range t.subHandlers {
		_ = handler.Start()
	}

	return nil
}

// Stop implements ext.Trigger.Stop
func (t *Trigger) Stop() error {

	for _, handler := range t.subHandlers {
		_ = handler.Stop()
	}

	_ = t.conn.Stop()
	return nil
}

// func (h *Handler) consumePartition(consumer sarama.PartitionConsumer) {
// 	for {
// 		select {
// 		case err := <-consumer.Errors():
// 			if err == nil {
// 				//was shutdown
// 				return
// 			}
// 			time.Sleep(time.Millisecond * 100)
// 		case <-h.shutdown:
// 			return
// 		case msg := <-consumer.Messages():

// 			if h.logger.DebugEnabled() {
// 				h.logger.Debugf("Kafka subscriber triggering action from topic [%s] on partition [%d] with key [%s] at offset [%d]",
// 					msg.Topic, msg.Partition, msg.Key, msg.Offset)

// 				h.logger.Debugf("Kafka message: '%s'", string(msg.Value))
// 			}

// 			out := &Output{}
// 			out.Message = string(msg.Value)

// 			_, err := h.handler.Handle(context.Background(), out)
// 			if err != nil {
// 				h.logger.Errorf("Run action for handler [%s] failed for reason [%s] message lost", h.handler.Name(), err)
// 			}
// 		}
// 	}
// }

func (h *Handler) fetchMessage(stream string) {
	for {
		select {
		case <-h.shutdown:
			return
		default:
			streams, err := h.conn.XReadStreams(h.conn.Context(), stream).Result()
			if err != nil {
				h.logger.Errorf("fetchMessage action for handler [%s] failed for reason [%s]", h.handler.Name(), err)
				time.Sleep(time.Millisecond * 100)
			}

			for streamIdx, xstream := range streams {
				streamName := xstream.Stream
				for msgIdx, xmsg := range xstream.Messages {
					msgID := xmsg.ID
					jsonBytes, err := json.Marshal(xmsg.Values)
					if err != nil {
						h.logger.Errorf("fetchMessage action for handler [%s] failed for reason [%s]", h.handler.Name(), err)
					} else {
						fmt.Println("Stream: ", streamName, " #: ", streamIdx, " Message idx: ", msgIdx, " Message ID: ", msgID)
						if h.logger.DebugEnabled() {
							h.logger.Debugf("Stream: '%s' (#%d), Message #%d, ID: %s, Content: %s", streamName, streamIdx, msgIdx, msgID, string(jsonBytes))
						}
						fmt.Printf("Stream: '%s' (#%d), Message #%d, ID: %s, Content: %s\n", streamName, streamIdx, msgIdx, msgID, string(jsonBytes))

						out := &Output{}
						out.Stream = streamName
						out.Message = string(jsonBytes)
						_, err := h.handler.Handle(context.Background(), out)
						if err != nil {
							h.logger.Errorf("Run action for handler [%s] failed for reason [%s] message lost", h.handler.Name(), err)
						}
					}

				}
			}
		}
	}
}

// Start starts the handler
func (h *Handler) Start() error {

	for _, stream := range h.streams {
		go h.fetchMessage(stream)
	}

	return nil
}

// Stop stops the handler
func (h *Handler) Stop() error {
	h.shutdown <- true
	close(h.shutdown)

	// for _, consumer := range h.streams {
	// 	_ = consumer.Close()
	// }
	return nil
}
