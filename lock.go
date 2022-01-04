package main

import (
	"context"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
)

const DistributedLockKey = "update_distributed_lock"

type DistributedLock struct {
	BlockingTime     int64 //wait for get lock (s)   10 * time.Hour
	LeaseRenewalTime int64 //lease renewal time  30 * time.Minute
	HoldingTime      int64 //holding time  1h
	RedisPool        *redis.Pool
	Key              string
	CancelFunc       context.CancelFunc
	Ctx              context.Context
}

func NewDistributedLock(redisPool *redis.Pool, key string, blockingTime int64, leaseRenewalTime int64) *DistributedLock {
	ctx, cancel := context.WithCancel(context.Background())
	return &DistributedLock{
		BlockingTime:     blockingTime,     //10 * 60 * 60
		LeaseRenewalTime: leaseRenewalTime, //30 * 60
		RedisPool:        redisPool,
		Key:              key,
		Ctx:              ctx,
		CancelFunc:       cancel,
	}
}

func (lock *DistributedLock) Lock(timeoutCtx context.Context) error {
	for {
		select {
		case <-timeoutCtx.Done():
			return timeoutCtx.Err()
		default:
		}

		lockKey, err := lock.getDistributedLock()
		if err != nil && strings.Contains(err.Error(), "nil returned") {
			lock.setDistributedLock(lock.Key)
			continue
		}
		if err != nil {
			log.Errorf("[GetDistributedLock] get redis error:%v", err)
		}
		if lock.Key != lockKey {
			time.Sleep(time.Duration(lock.BlockingTime) * time.Second)
			continue
		}
		//Acquire the lock immediately to prevent lock expiration or goroutine timeout scheduling
		flag := lock.leaseRenewal(lock.Key)
		if !flag {
			continue
		}
		go func() {
			c := time.Tick(time.Duration(lock.LeaseRenewalTime) * time.Second)
			for {
				select {
				case <-lock.Ctx.Done():
					return
				case <-c:
					flag := lock.leaseRenewal(lock.Key)
					if !flag {
						log.Errorf("leaseRenewal error")
					}
					// default:
					// 	time.Sleep(time.Duration(lock.LeaseRenewalTime/4) * time.Second)
				}
			}
		}()
		return nil
	}
}

//release lock and sleep logic
func (lock *DistributedLock) Unlock() {
	lock.CancelFunc()
	flag := lock.releaseDistributedLock(lock.Key)
	if !flag {
		log.Errorf("release lock error")
	}
}

func (p *DistributedLock) releaseDistributedLock(podName string) bool {
	redisConn := p.RedisPool.Get()
	defer func() {
		_ = redisConn.Close()
	}()
	script := redis.NewScript(1, `
	if redis.call('get', KEYS[1]) == ARGV[1]
	then
		return redis.call('del', KEYS[1])
	else
		return 0
	end
	`)
	resp, err := script.Do(redisConn, DistributedLockKey, podName)
	if err != nil {
		log.Errorf("[LeaseRenewal] error:%v", err)
	}
	if num, ok := resp.(int64); ok && num == 1 {
		return true
	}
	return false
}

func (p *DistributedLock) getDistributedLock() (string, error) {
	redisConn := p.RedisPool.Get()
	defer func() {
		_ = redisConn.Close()
	}()
	data, err := redis.String(redisConn.Do("GET", DistributedLockKey))
	if err != nil {
		return "", err
	}
	return data, nil
}

func (p *DistributedLock) setDistributedLock(podName string) bool {
	redisConn := p.RedisPool.Get()
	defer func() {
		_ = redisConn.Close()
	}()
	res, err := redisConn.Do("SET", DistributedLockKey, podName, "ex", 60*60, "nx")
	if err != nil {
		log.Errorf("[SetDistributedLock] set error:%v", err)
		return false
	}
	if res == nil {
		log.Error("[SetDistributedLock] set error:already lock")
		return false
	}
	return true
}

func (p *DistributedLock) leaseRenewal(podName string) bool {
	redisConn := p.RedisPool.Get()
	defer func() {
		_ = redisConn.Close()
	}()
	script := redis.NewScript(1, `
	if redis.call('get', KEYS[1]) == ARGV[1]
	then
		return redis.call('expire', KEYS[1], 3600)
	else
		return 0
	end
	`)
	resp, err := script.Do(redisConn, DistributedLockKey, podName)
	if err != nil {
		log.Errorf("[LeaseRenewal] error:%v", err)
	}
	if num, ok := resp.(int64); ok && num == 1 {
		return true
	}
	return false
}

