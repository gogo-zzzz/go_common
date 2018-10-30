package db

import (
	"fmt"
	"sync"

	"gopkg.in/mgo.v2"
)

type MongoDbClient struct {
	Client *mgo.Session
	Addr   string
	Port   int
	User   string
	Pwd    string
	Db     string
}

//	[mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]
//	mongodb://myuser:mypass@localhost:40001,otherhost:40001/mydb
func (self *MongoDbClient) InitByString(con string, ping bool) error {
	var err error
	self.Client, err = mgo.Dial(con)
	if err != nil {
		panic(err)
	}

	err = self.Client.Ping()
	if err != nil {
		fmt.Println("mongodb.InitByString ping db failed:", err)
		return err
	}

	fmt.Println("mongodb.InitByString ping db ok")

	return err
}

func (self *MongoDbClient) Close() {
	self.Client.Close()
	fmt.Println("MysqlClient closed")
}

type MongoDbClientPool struct {
	sync.Mutex
	pool   []*MongoDbClient
	intUse int
}

var GlobalMongoDbClientPool MongoDbClientPool

/**
@param str="ip:port password dbindex"
*/
func (self *MongoDbClientPool) InitFromString(str string, cap int, pingTest bool, breadIfError bool) error {
	self.Lock()
	defer self.Unlock()
	self.pool = make([]*MongoDbClient, cap)
	fmt.Println("MongoDbClientPool.Init begin, capacity is", cap)

	for i := 0; i < cap; i++ {
		var err error
		self.pool[i] = &MongoDbClient{}
		//if !self.pool[i].Info.ParseFromString(str) {
		self.pool[i].Client, err = mgo.Dial(str)
		if err != nil {
			fmt.Println("mongodb.InitByString(", str, ") ping db failed:", err)
			panic(err)
		}

		err = self.pool[i].Client.Ping()
		if err != nil {
			fmt.Println("mongodb.InitByString ping db failed:", err)
			return err
		}
		//mongoDbClient := self.pool[i]
		//mongoDbClient.Pool = self
	}

	return nil
}

func (self *MongoDbClientPool) GetClient() *MongoDbClient {
	self.Lock()
	defer self.Unlock()

	if len(self.pool) < 1 {
		fmt.Println("MongoDbClientPool.GetClient no free client")
		return nil
	}

	index := len(self.pool) - 1
	client := self.pool[index]
	self.pool = self.pool[:index]

	//self.poolInUse = append(self.poolInUse, client)

	self.intUse += 1

	fmt.Println("MongoDbClientPool.GetClient in use ", self.intUse)

	return client
}

func (self *MongoDbClientPool) ReturnClient(mongoDbClient *MongoDbClient) {
	self.Lock()
	defer self.Unlock()

	self.pool = append(self.pool, mongoDbClient)

	self.intUse -= 1

	fmt.Println("MongoDbClientPool.ReturnClient in use ", self.intUse)
}

func (self *MongoDbClientPool) GetCanUseCount() {
	self.Lock()
	defer self.Unlock()

	return len(self.pool)
}
