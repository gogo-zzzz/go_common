//  gowinder@hotmail.com 2017/7/5 9:05
package db

import (
	"github.com/go-redis/redis"
	"fmt"
	"sync"
	"log"
	"errors"
)

type RedisInfo struct {
	Addr string
	Pwd  string
	Db   int
}

/**
@param str="ip:port password dbindex"
 */
func (self *RedisInfo) ParseFromString(str string) bool {
	n, err := fmt.Sscanf(str, "%s %s %d", &self.Addr, &self.Pwd, &self.Db)
	if err != nil {
		log.Printf("ParseFromString %s error %v", str, err)
		return false
	}
	if n != 3 {
		log.Printf("ParseFromString %s read not match", str)
		return false
	}

	return true
}

type RedisClient struct{
	Client 	*redis.Client
	Info	RedisInfo

	Pool	*RedisClientPool
}

func (self *RedisClient) Init(ping bool) error {
	self.Client = redis.NewClient(&redis.Options{
		Addr:     self.Info.Addr,
		Password: self.Info.Pwd, // no password set
		DB:       self.Info.Db,  // use default DB
	})

	if ping{
		_, err := self.Client.Ping().Result()
		if err == nil{
			fmt.Println("RedisClient.Init ping", self.Info.Addr, "ok")
		}else {
			fmt.Println("RedisClient.Init ping", self.Info.Addr, "failed, err:", err)
			return err
		}
	}

	return nil
}


func (self *RedisClient) Close(){
	self.Client.Close()
	fmt.Println("RedisClient closed")
}

func (self *RedisClient) ReturnToPool(){
	if self.Pool != nil{
		self.Pool.ReturnClient(self)
	}
}

func (self *RedisClient) MultiGet(keys []string) *redis.SliceCmd {
	args := make([]interface{}, 1 + len(keys))
	args[0] = "mget"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := redis.NewSliceCmd(args...)
	self.Client.Process(cmd)
	return cmd
}

func (self *RedisClient) MultiDel(keys []string) *redis.IntCmd {
	self.Client.Del()
	args := make([]interface{}, 1 + len(keys))
	args[0] = "del"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := redis.NewIntCmd(args...)
	self.Client.Process(cmd)
	return cmd
}



type RedisClientPool struct{
	sync.Mutex
	pool	 []*RedisClient
	//	poolInUse	[]*RedisClient
	intUse	int
}

var GlobalRedisClientPool	RedisClientPool

func (self *RedisClientPool) Init(addr string, pwd string, db int, cap int, pingTest bool, breadIfError bool) error{
	self.Lock()
	defer self.Unlock()
	self.pool = make([]*RedisClient, cap)
	fmt.Println("RedisClientPool.Init begin, capacity is", cap)

	for i := 0; i < cap; i++{
		self.pool[i] = &RedisClient{ Info:RedisInfo{Addr:addr,Pwd:pwd,Db:db}}
		redisClient := self.pool[i]
		redisClient.Pool = self

		err := redisClient.Init(pingTest)
		if err != nil && breadIfError{
			return err
		}
	}

	return nil
}

/**
@param str="ip:port password dbindex"
 */
func (self *RedisClientPool) InitFromString(str string, cap int, pingTest bool, breadIfError bool) error{
	self.Lock()
	defer self.Unlock()
	self.pool = make([]*RedisClient, cap)
	fmt.Println("RedisClientPool.Init begin, capacity is", cap)

	for i := 0; i < cap; i++{
		self.pool[i] = &RedisClient{ Info:RedisInfo{}}
		if !self.pool[i].Info.ParseFromString(str){
			return errors.New("RedisClientPool.InitFromString failed")
		}
		redisClient := self.pool[i]
		redisClient.Pool = self

		err := redisClient.Init(pingTest)
		if err != nil && breadIfError{
			return err
		}
	}

	return nil
}

func (self *RedisClientPool) GetClient() *RedisClient{
	self.Lock()
	defer self.Unlock()

	if len(self.pool) < 1{
		fmt.Println("RedisClientPool.GetClient no free client")
		return nil
	}

	index := len(self.pool) - 1
	client := self.pool[index]
	self.pool = self.pool[:index]

	//self.poolInUse = append(self.poolInUse, client)

	self.intUse += 1

	return client
}

func (self *RedisClientPool) ReturnClient(redisClient *RedisClient) {
	self.Lock()
	defer self.Unlock()


	self.pool = append(self.pool, redisClient)

	self.intUse -= 1
}
