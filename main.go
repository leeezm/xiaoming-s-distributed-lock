package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
)

func main() {
	redisPool := &redis.Pool{
		MaxIdle:   80,
		MaxActive: 1000,
		Dial: func() (redis.Conn, error) {
			hostPort := fmt.Sprintf("%s:%d", "0.0.0.0", 6379)
			c, err := redis.Dial("tcp", hostPort, redis.DialConnectTimeout(time.Millisecond*150),
				redis.DialReadTimeout(time.Millisecond*150),
				redis.DialWriteTimeout(time.Millisecond*150))
			return c, err
		},
	}
	var cliName = flag.String("name", "Name1", "")
	flag.Parse()
	distributedLock := NewDistributedLock(redisPool, *cliName, 3, 30*60)
	ctx, _ := context.WithTimeout(context.Background(), 20*time.Second)
	err := distributedLock.Lock(ctx)
	if err != nil {
		log.Errorf("get lock error:%v", err)
	} else {
		for i := 0; i < 4; i++ {
			log.Infof("[test] get lock,Lock Name:%v", *cliName)
			time.Sleep(5 * time.Second)
		}
		distributedLock.Unlock()
	}
}
