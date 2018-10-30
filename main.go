//  gowinder@hotmail.com 2017/7/5 9:22
package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"go_common/db"
)

var redisAddr string = "127.0.0.1:6379"
var redisPwd string = "asdf"
var redisDb int = 0

var mysqlAddr string = "127.0.0.1"
var mysqlUser string = "root"
var mysqlPwd	string = "asdf"
var mysqlPort	int = 3306
var mysqlDb string = "test"


const VERSION string = "0.2.3"

func main() {
	fmt.Println("start svpn cache gate  version ", VERSION)


	//testRedis()
	//
	//err := db.GlobalRedisClientPool.Init(redisAddr, redisPwd, redisDb, 50, true, true)
	//if err != nil {
	//	fmt.Println("GlobalRedisClientPool.Init error ", err)
	//	return
	//}



	testMongoString()
	testRedisString()
	testMysqlString()

	err := db.GlobalRedisClientPool.InitFromString("192.168.121.2:6379 asdf 0", 50, true, true)
	if err != nil {
		fmt.Println("GlobalRedisClientPool.Init error ", err)
		return
	}


}
func testMongoString() {
	client := &db.MongoDbClient{}

	err := client.InitByString("mongodb://192.168.121.2:27017/test", true)
	if err != nil {
		fmt.Println("testMongoString init failed, ", err)
	}

	err = client.Client.DB("").C("tt").Insert(&db.MongoDbClient{})
	if err != nil {
		fmt.Println("testMongoString insert failed, ", err)
	}

}
func testMysql() {
	client := &db.MysqlClient{Addr: mysqlAddr, User: mysqlUser, Pwd: mysqlPwd, Port: mysqlPort, Db: mysqlDb}
	err := client.Init(false)
	if err != nil {
		fmt.Println("testMysql init failed, ", err)
	}

	rows, err := client.Client.Query("select count(0) from testdata")
	if err != nil {
		fmt.Println("testmysql query failed, ", err)
	}

	var count int
	if err := rows.Scan(&count); err != nil {
		fmt.Println("testmysql scan row failed, ", err)
	}
	fmt.Println("testmysql scan row result: ", count)
}

func testMysqlString() {
	client := &db.MysqlClient{}

	err := client.InitByString("root:asdf@tcp(192.168.121.2:3306)/svpn_log?charset=utf8", true)
	if err != nil {
		fmt.Println("testMysql init failed, ", err)
	}

	rows, err := client.Client.Query("select count(0) from user_proxy_logs")
	if err != nil {
		fmt.Println("testmysql query failed, ", err)
	}

	var count int
	if err := rows.Scan(&count); err != nil {
		fmt.Println("testmysql scan row failed, ", err)
	}
	fmt.Println("testmysql scan row result: ", count)
}

func testRedis() {
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPwd, // no password set
		DB:       redisDb,  // use default DB
	})

	_, err := client.Ping().Result()
	if err == nil{
		fmt.Println("test redis", redisAddr, "ok")
	}else {
		fmt.Println("test redis", redisAddr, "failed, err:", err)
	}

	err = client.Set("testkey", "mother fucker", 0).Err()
	if err != nil {
		panic(err)
	}

}

func testRedisString() {
	rc := &db.RedisClient{}
	if !rc.Info.ParseFromString("192.168.121.2:6379 asdf 1"){
		panic("testRedisString failed")
	}

	rc.Init(true)

	//client := redis.NewClient(&redis.Options{
	//	Addr:     rc.Info.Addr,
	//	Password: rc.Info.Pwd, // no password set
	//	DB:       rc.Info.Db,  // use default DB
	//})

	err := rc.Init(true)
	if err == nil{
		fmt.Println("test redis", redisAddr, "ok")
	}else {
		fmt.Println("test redis", redisAddr, "failed, err:", err)
	}

	err = rc.Client.Set("testkey", "mother fucker", 0).Err()
	if err != nil {
		panic(err)
	}

}