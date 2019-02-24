package db_test

import (
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/gogo-zzzz/go_common/db"
)

var _ = Describe("pool", func() {
	var pool db.RedisClientPool
	const testKey string = "go_common_test_key"

	type testKeyValue struct {
		TestTime time.Time
		X        int
		S        string
	}

	testValue := testKeyValue{
		TestTime: time.Now(),
		X:        1234,
		S:        "asdf",
	}
	b, _ := json.Marshal(testValue)
	testValueString := string(b)

	expiration := time.Second * 5

	BeforeEach(func() {
		pool.InitFromString("192.168.2.18:30067 redis_passwd 0", 20, true, true)
	})

	AfterEach(func() {
	})

	// It("should connect", func() {
	// 	err := pool.InitFromString("192.168.2.18:30067 redis_passwd 0", 20, true, true)
	// 	Ω(err).ShouldNot(HaveOccurred())
	// })

	Context("get set test", func() {
		It("should set ok", func() {
			ret := pool.RedisSet(testKey, testValueString, 0, "test_set")
			Expect(ret).To(Equal(true))
		})

		It("should get ok", func() {
			ret, valueString := pool.RedisGet(testKey, "test_get")
			Expect(ret).To(Equal(true))
			Expect(valueString).To(Equal(testValueString))
		})
	})

	It("should expired", func() {
		ret := pool.RedisSet(testKey, testValueString, expiration, "test_expire")
		Expect(ret).To(Equal(true))

		time.Sleep(expiration + time.Second)
		ret, _ = pool.RedisGet(testKey, "test_expire")
		Expect(ret).To(Equal(false))
	})
})
