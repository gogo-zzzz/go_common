package db

import (
	"fmt"
	"gopkg.in/mgo.v2"
)

type MongoDbClient struct{
	Client	*mgo.Session
	Addr	string
	Port	int
	User	string
	Pwd		string
	Db		string
}


//	[mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]
//	mongodb://myuser:mypass@localhost:40001,otherhost:40001/mydb
func (self *MongoDbClient) InitByString(con string, ping bool) error{
	var err error
	self.Client, err = mgo.Dial(con)
	if err != nil {
		panic(err)
	}

	err = self.Client.Ping()
	if err != nil{
		fmt.Println("mongodb.InitByString ping db failed:", err)
		return err
	}

	fmt.Println("mongodb.InitByString ping db ok")

	return err
}

func (self *MongoDbClient) Close(){
	self.Client.Close()
	fmt.Println("MysqlClient closed")
}
