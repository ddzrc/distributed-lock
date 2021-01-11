package distributed_lock


import (
	"fmt"
	"sync"
	"testing"
)



func TestLock(t *testing.T) {

	key := "key.redis.lock.test"
	group := sync.WaitGroup{}
	for i := 0; i < 1; i++ {
		group.Add(1)
		go func() {
			keyList := make([]string, 0)
			for j := 0; j < 200; j ++ {
				keyValue := key + fmt.Sprint(j)
				keyList = append(keyList, keyValue)
			}
			lockList, err := redisLock.MultiLock(keyList, 10000)
			if err != nil {
				fmt.Println(err)
			}

			lockList, err = redisLock.MultiUnLock(lockList)
			if err != nil {
				fmt.Println(err)
			}

			group.Done()
		}()
	}
	group.Wait()
}
