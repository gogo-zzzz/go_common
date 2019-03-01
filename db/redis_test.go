package db_test

import (
	"encoding/json"
	"time"

	"github.com/kataras/golog"

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

	Context("mulit-del", func() {
		keys := []string{"test_multi_del.a", "test_multi_del.b", "test_multi_del.c", "test_multi_del.d"}
		It("should set ok", func() {
			for _, key := range keys {
				ret := pool.RedisSet(key, testValueString, 0, key)
				Expect(ret).To(Equal(true))
			}
		})

		It("should del ok", func() {
			ret := pool.RedisMultiDel(keys, "test_multi_del")
			Expect(ret).To(Equal(true))
		})
	})

	Context("mulit-get", func() {
		keys := []string{"test.a.b.c", "test.a.b.c.d", "test.a.b.c.d.e", "test.a.b.2.e"}
		values := []string{"asbc", "1234", "bck234kj", "124123kj"}
		BeforeEach(func() {
			for i, key := range keys {
				pool.RedisSet(key, values[i], 0, " redis_keys_test ")
			}
		})

		AfterEach(func() {
			pool.RedisMultiDel(keys, "mulit-get-after-each")
		})

		It("should multiget ok", func() {
			ret, result := pool.RedisMultiGet(keys, " multiget test")
			Expect(ret).To(Equal(true))
			for i, _ := range result {
				Expect(result[i]).To(Equal(values[i]))
			}
		})

	})

	It("should expired", func() {
		ret := pool.RedisSet(testKey, testValueString, expiration, "test_expire")
		Expect(ret).To(Equal(true))

		time.Sleep(expiration + time.Second)
		ret, _ = pool.RedisGet(testKey, "test_expire")
		Expect(ret).To(Equal(false))
	})

	Context("get keys test", func() {
		keys := []string{"test.a.b.c", "test.a.b.c.d", "test.a.b.c.d.e", "test.a.b.2.e"}
		BeforeEach(func() {
			for _, key := range keys {
				pool.RedisSet(key, "xx", 0, " redis_keys_test ")
			}
		})

		AfterEach(func() {
			pool.RedisMultiDel(keys, " redis_keys_test ")
		})

		It("should get all keys", func() {
			ret, result := pool.RedisKeys("test.a.b.c*", "get_keys_test")
			Expect(ret).To(Equal(true))
			if len(result) != 3 {
				golog.Info(result)
			}
			Expect(len(result)).To(Equal(3))
		})
	})

	It("should incr", func() {
		test_incr := "test_incr"
		ok, val := pool.RedisIncr(test_incr, "incr_test")
		Expect(ok).Should(BeTrue())
		ok, val2 := pool.RedisIncr(test_incr, "incr_test")
		Expect(ok).Should(BeTrue())
		Expect(val + 1).Should(Equal(val2))
	})
})
