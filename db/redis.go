package db

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/kataras/golog"
)

/*
RedisInfo redis server info
*/
type RedisInfo struct {
	Addr string
	Pwd  string
	Db   int
}

/*
ParseFromString parse redis server from string
@param str="ip:port password dbindex"
*/
func (info *RedisInfo) ParseFromString(str string) bool {
	n, err := fmt.Sscanf(str, "%s %s %d", &info.Addr, &info.Pwd, &info.Db)
	if n < 2 && err != nil {
		log.Printf("ParseFromString %s error %v", str, err)
		return false
	}

	if n != 3 {
		if n == 2 {
			info.Db, _ = strconv.Atoi(info.Pwd)
			info.Pwd = ""
			return true
		}
		log.Printf("ParseFromString %s read not match", str)
		return false
	}

	return true
}

/*
RedisClient redis client
*/
type RedisClient struct {
	Client *redis.Client
	Info   RedisInfo

	Pool *RedisClientPool
}

/*
Init init a redis client object
*/
func (client *RedisClient) Init(ping bool) error {
	client.Client = redis.NewClient(&redis.Options{
		Addr:     client.Info.Addr,
		Password: client.Info.Pwd, // no password set
		DB:       client.Info.Db,  // use default DB
	})

	if ping {
		_, err := client.Client.Ping().Result()
		if err == nil {
			fmt.Println("RedisClient.Init ping", client.Info.Addr, "ok")
		} else {
			fmt.Println("RedisClient.Init ping", client.Info.Addr, "failed, err:", err)
			return err
		}
	}

	return nil
}

/*
Close close a redis client session
*/
func (client *RedisClient) Close() {
	client.Client.Close()
	fmt.Println("RedisClient closed")
}

/*
ReturnToPool return redis client to pool
*/
func (client *RedisClient) ReturnToPool() {
	if client.Pool != nil {
		client.Pool.ReturnClient(client)
	}
}

/*
MultiGet multiply get
*/
func (client *RedisClient) MultiGet(keys []string) *redis.SliceCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "mget"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := redis.NewSliceCmd(args...)
	client.Client.Process(cmd)
	return cmd
}

/*
MultiDel  multiply delete
*/
func (client *RedisClient) MultiDel(keys []string) *redis.IntCmd {
	client.Client.Del()
	args := make([]interface{}, 1+len(keys))
	args[0] = "del"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := redis.NewIntCmd(args...)
	client.Client.Process(cmd)
	return cmd
}

/*
Get get key value
*/
func (client *RedisClient) Get(key string, logName string) (ret bool, result string) {
	ret = false
	cmd := client.Client.Get(key)
	if cmd == nil {
		golog.Error(logName, " get key ", key, " return nil")
		return
	}

	result, err := cmd.Result()
	if err != nil {
		golog.Error(logName, " get key ", key, " error", cmd.Err())
		return
	}

	ret = true
	return
}

/*
Set set key value
*/
func (client *RedisClient) Set(key string, value interface{}, expiration time.Duration, logName string) (ret bool) {
	ret = false
	cmd := client.Client.Set(key, value, expiration)
	if cmd == nil {
		golog.Error(logName, " RedisClient.Set: key ", key, " return nil")
		return
	}

	_, err := cmd.Result()
	if err != nil {
		golog.Error(logName, " RedisClient.Set: key ", key, " error", cmd.Err())
		return
	}

	ret = true
	return
}

/*
Keys get keys by pattern
*/
func (client *RedisClient) Keys(keyPattern string, logName string) (ret bool, result []string) {
	ret = false
	cmd := client.Client.Keys(keyPattern)
	if cmd == nil {
		golog.Error(logName, " RedisClient.Keys: keyPattern ", keyPattern, " return nil")
		return
	}

	result, err := cmd.Result()
	if err != nil {
		golog.Error(logName, " RedisClient.Keys: keyPattern ", keyPattern, " error", cmd.Err())
		return
	}

	ret = true
	return
}

/*
Exists key exist
*/
func (client *RedisClient) Exists(key string, logName string) bool {
	cmd := client.Client.Exists(key)
	if cmd == nil {
		golog.Error(logName, " get key ", key, " return nil")
		return false
	}

	result, err := cmd.Result()
	if err != nil {
		golog.Error(logName, " get key ", key, " error", cmd.Err())
		return false
	}

	if result == 1 {
		return true
	}
	return false
}

/*
Keys get keys by pattern
*/
func (client *RedisClient) Incr(key string, logName string) (ret bool, val int64) {
	ret = false
	cmd := client.Client.Incr(key)
	if cmd == nil {
		golog.Error(logName, " RedisClient.Incr: key ", key, " return nil")
		return
	}

	val, err := cmd.Result()
	if err != nil {
		golog.Error(logName, " RedisClient.Incr: key ", key, " error", cmd.Err())
		return
	}

	ret = true
	return
}

/*
RedisClientPool redis client pool
*/
type RedisClientPool struct {
	sync.Mutex
	pool []*RedisClient
	//	poolInUse	[]*RedisClient
	intUse int
}

/*
GlobalRedisClientPool global redis client pool
*/
var GlobalRedisClientPool RedisClientPool

/*
Init initialize a pool
*/
func (pool *RedisClientPool) Init(addr string, pwd string, db int, cap int, pingTest bool, breakIfError bool) error {
	pool.Lock()
	defer pool.Unlock()
	pool.pool = make([]*RedisClient, cap)
	fmt.Println("RedisClientPool.Init begin, capacity is", cap)

	for i := 0; i < cap; i++ {
		pool.pool[i] = &RedisClient{Info: RedisInfo{Addr: addr, Pwd: pwd, Db: db}}
		redisClient := pool.pool[i]
		redisClient.Pool = pool

		err := redisClient.Init(pingTest)
		if err != nil && breakIfError {
			return err
		}
	}

	return nil
}

/*
InitFromString init pool from string
@param str="ip:port password dbindex"
*/
func (pool *RedisClientPool) InitFromString(str string, cap int, pingTest bool, breadIfError bool) error {
	pool.Lock()
	defer pool.Unlock()
	pool.pool = make([]*RedisClient, cap)
	fmt.Println("RedisClientPool.Init begin, capacity is ", cap)

	for i := 0; i < cap; i++ {
		pool.pool[i] = &RedisClient{Info: RedisInfo{}}
		if !pool.pool[i].Info.ParseFromString(str) {
			return errors.New("RedisClientPool.InitFromString failed")
		}
		redisClient := pool.pool[i]
		redisClient.Pool = pool

		err := redisClient.Init(pingTest)
		if err != nil && breadIfError {
			return err
		}
	}

	return nil
}

/*
GetClient get one client from pool
*/
func (pool *RedisClientPool) GetClient() *RedisClient {
	pool.Lock()
	defer pool.Unlock()

	if len(pool.pool) < 1 {
		fmt.Println("RedisClientPool.GetClient no free client")
		return nil
	}

	index := len(pool.pool) - 1
	client := pool.pool[index]
	pool.pool = pool.pool[:index]

	//pool.poolInUse = append(pool.poolInUse, client)

	pool.intUse++

	return client
}

/*
ReturnClient return client to pool
*/
func (pool *RedisClientPool) ReturnClient(redisClient *RedisClient) {
	pool.Lock()
	defer pool.Unlock()

	pool.pool = append(pool.pool, redisClient)

	pool.intUse--
}

/*
RedisGet at first get one avariable redis client from pool,
	then get key value from redis
*/
func (pool *RedisClientPool) RedisGet(key string, logName string) (ret bool, result string) {
	ret = false
	result = ""
	redisClient := pool.GetClient()
	if redisClient == nil {
		golog.Error(" RedisGet connect redis failed")
		return
	}
	defer redisClient.ReturnToPool()

	ret, result = redisClient.Get(key, logName)

	return
}

/*
RedisSet at first get one avariable redis client from pool,
	then set key value to redis
*/
func (pool *RedisClientPool) RedisSet(key string, value string, expiration time.Duration, logName string) (ret bool) {
	ret = false
	redisClient := pool.GetClient()
	if redisClient == nil {
		golog.Error(" RedisSet connect redis failed")
		return
	}
	defer redisClient.ReturnToPool()

	ret = redisClient.Set(key, value, expiration, logName)
	return
}

/*
RedisKeys return redis key by pattern
*/
func (pool *RedisClientPool) RedisKeys(keyPattern string, logName string) (ret bool, result []string) {
	ret = false
	redisClient := pool.GetClient()
	if redisClient == nil {
		golog.Error(" RedisKeys connect redis failed")
		return
	}
	defer redisClient.ReturnToPool()

	ret, result = redisClient.Keys(keyPattern, logName)

	return
}

/*
RedisDel delete keys
*/
func (pool *RedisClientPool) RedisMultiDel(keys []string, logName string) (ret bool) {
	ret = false
	redisClient := pool.GetClient()
	if redisClient == nil {
		golog.Error("RedisKeys connect redis failed")
		return
	}
	defer redisClient.ReturnToPool()

	if redisClient.MultiDel(keys).Err() != nil {
		golog.Error("RedisClientPool.RedisMultiDel(", logName, ") err:", redisClient.MultiDel(keys).Err())
		ret = false
	} else {
		ret = true
	}

	return
}

/*
RedisMultiGet multi-get redis key
*/
func (pool *RedisClientPool) RedisMultiGet(keys []string, logName string) (ret bool, result []interface{}) {
	ret = false
	redisClient := pool.GetClient()
	if redisClient == nil {
		golog.Error("RediRedisMultiGetsKeys connect redis failed")
		return
	}
	defer redisClient.ReturnToPool()

	cmd := redisClient.MultiGet(keys)
	if cmd.Err() != nil {
		golog.Error("RedisClientPool.RedisMultiGet(", logName, ") err:", cmd.Err())
		ret = false
	} else {
		result, _ = cmd.Result()
		ret = true
	}

	return
}

/*
RedisExists check key exists
*/
func (pool *RedisClientPool) RedisExists(key string, logName string) (ret bool) {
	redisClient := pool.GetClient()
	if redisClient == nil {
		golog.Error(" RedisExists connect redis failed")
		return
	}
	defer redisClient.ReturnToPool()

	return redisClient.Exists(key, logName)
}

/*
RedisIncr increment key
*/
func (pool *RedisClientPool) RedisIncr(key string, logName string) (ret bool, val int64) {
	ret = false
	redisClient := pool.GetClient()
	if redisClient == nil {
		golog.Error("RedisClientPool.RedisIncr connect redis failed")
		return
	}
	defer redisClient.ReturnToPool()

	return redisClient.Incr(key, logName)
}
