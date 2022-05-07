package connection

import (
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/project-flogo/core/support/log"
)

type Settings struct {
	uri      string `md:"Redis Connection URL,required"` // The Redis server to connect to
	db       int    `md:"Database Index"`
	username string `md:"Username"` // If connecting to a SASL enabled port, the user id to use for authentication
	password string `md:"Password"` // If connecting to a SASL enabled port, the password to use for authentication
}

type RedisConnection interface {
	Client() interface{}
	Stop() error
}
type RedisConnect struct {
	options *redis.Options
	client  *redis.Client
}

func (c *RedisConnect) Client() interface{} {
	return c.client
}

func (c *RedisConnect) Stop() error {
	err := c.client.Close()
	if err != nil {
		return err
	}
	return nil

}

func getConnectionKey(settings *Settings) string {
	return fmt.Sprintf("%s:%s@%s/%d", settings.username, settings.password, settings.uri, settings.db)
}

func getRedisConnection(logger log.Logger, settings *Settings) (RedisConnection, error) {

	newConn := &RedisConnect{}
	newConn.options.Addr = settings.uri
	newConn.options.DB = 0
	newConn.options.Username = settings.username
	newConn.options.Password = settings.password
	if settings.db > 0 {
		newConn.options.DB = settings.db
	}

	logger.Debugf("Redis url: [%s]", newConn.options.Addr)
	client := redis.NewClient(newConn.options)
	newConn.client = client

	return newConn, nil
}
