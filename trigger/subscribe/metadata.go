package xread

import (
	"github.com/project-flogo/core/data/coerce"
	"github.com/project-flogo/core/support/connection"
)

type Settings struct {
	Connection connection.Manager `md:"connection,required"`
}
type HandlerSettings struct {
	Streams string `md:"streams,required"` // The Redis streams on which to listen for messages
}

type Output struct {
	Stream  string `md:"stream"`  // The stream from which the message was received
	Message string `md:"message"` // The message that was consumed
}

func (o *Output) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"stream":  o.Stream,
		"message": o.Message,
	}
}

func (o *Output) FromMap(values map[string]interface{}) error {

	var err error
	o.Message, err = coerce.ToString(values["message"])
	if err != nil {
		return err
	}

	o.Stream, err = coerce.ToString(values["stream"])
	if err != nil {
		return err
	}

	return nil
}
