package redis

import (
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/connection"
	"github.com/project-flogo/core/support/log"
)

func init() {
	connection.RegisterManager("redisconnection", &RedisSharedConn{})
	connection.RegisterManagerFactory(&Factory{})
}

type RedisSharedConn struct {
	conn RedisConnection
}
type Factory struct {
}

func (*Factory) Type() string {
	return "redis:go-redis"
}

func (*Factory) NewManager(settings map[string]interface{}) (connection.Manager, error) {
	settingStruct := &Settings{}
	err := metadata.MapToStruct(settings, settingStruct, true)
	if err != nil {
		return nil, err
	}

	conn, err := getRedisConnection(log.ChildLogger(log.RootLogger(), "redis-shared-conn"), settingStruct)
	if err != nil {
		//ctx.Logger().Errorf("Redis parameters initialization got error: [%s]", err.Error())
		return nil, err
	}

	sharedConn := &RedisSharedConn{conn: conn}

	return sharedConn, nil
}

func (h *RedisSharedConn) Type() string {

	return "redis:go-redis"
}

func (h *RedisSharedConn) GetConnection() interface{} {

	return h.conn
}

func (h *RedisSharedConn) ReleaseConnection(connection interface{}) {

}

func (h *RedisSharedConn) Stop() error {
	return h.conn.Stop()
}

func (h *RedisSharedConn) Start() error {
	return nil
}
