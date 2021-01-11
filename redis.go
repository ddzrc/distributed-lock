package distributed_lock

import (
	"errors"
	"github.com/garyburd/redigo/redis"
	"github.com/google/uuid"
	"sync"
	"time"
	"unsafe"
)

type redisLock struct {
	*redis.Pool
}

type RedisLock struct {
	Key      string
	KeyToken string
	Success  bool
	pool     *redis.Pool
	ExpireMs int
	close    chan bool
}

type LockToken struct {
	Key      string
	KeyToken string
}

func NewRedisLock(pool *redis.Pool) redisLock {
	return redisLock{Pool: pool}
}

type localLockElement struct {
	e    map[string]*lockElement
	lock sync.Mutex
}

type lockElement struct {
	count int64
	lock  *sync.Mutex
}

type localLock struct {
	LockMap [256] localLockElement
}

var localLocalVar localLock

func init() {
	localLocalVar = localLock{}
	for key, _ := range localLocalVar.LockMap {
		localLocalVar.LockMap[key] = localLockElement{e: make(map[string]*lockElement, 0)}
	}
}

func (l *localLock) lock(key string) *sync.Mutex {
	bytes := ConvertStrToByte(key)
	slot := 0
	for i := 0; i < len(bytes) && i < len(l.LockMap); i++ {
		slot += bytes[i] << i
	}
	l.LockMap[slot].lock.Lock()
	l.LockMap[slot].e[key].count++
	l.LockMap[slot].lock.Unlock()
	return l.LockMap[slot].e[key].lock
}

func (l *localLock) unlLock(key string) {
	bytes := ConvertStrToByte(key)
	slot := 0
	for i := 0; i < len(bytes) && i < len(l.LockMap); i++ {
		slot += bytes[i] << i
	}
	l.LockMap[slot].lock.Lock()
	l.LockMap[slot].e[key].count -= 1
	if l.LockMap[slot].e[key].count == 0 {
		delete(l.LockMap[slot].e, key)
	}
	l.LockMap[slot].lock.Unlock()
	return
}

func (r *redisLock) Lock(key string, expireMs int, delayWaitMs int) (*RedisLock, error) {
	localLocalVar.lock(key).Lock()
	return r.lock(key, expireMs)
}

func (r *redisLock) lock(key string, expireMs int) (*RedisLock, error) {
	localLocalVar.lock(key).Lock()
	c := r.Pool.Get()
	c.Close()
	UUID, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	token := UUID.String()
	reply, err := redis.String(c.Do("SET", key, token, "NX", "PX", expireMs))
	if err == redis.ErrNil {
		return nil, errors.New("get lock failed")
	}
	if err != nil {
		return nil, errors.New("get lock failed")
	}
	if reply != "OK" {
		return &RedisLock{}, nil
	}
	result := &RedisLock{}
	result.Key = key
	result.KeyToken = token
	result.Success = true
	return result, nil
}

const (
	unlockScript = `
		if redis.call("GET",KEYS[1]) == ARGV[1] then
			delResult = redis.call("DEL",KEYS[1])
			redis.call("publish", KEYS[2], "unlock")
		    return deResult
		else
		    return 0
		end
	`
)

func (r *RedisLock) UnLock(key string, token string, expireMs int) error {
	c := r.pool.Get()
	defer func() {
		localLocalVar.unlLock(key)
		c.Close()
	}()

	cmd := redis.NewScript(2, unlockScript)
	res, err := redis.Int(cmd.Do(c, key, token, getSubscribeChanel(key), "listen"), )
	if err != nil {
		return err
	}

	if res != 1 {
		return errors.New("解锁失败，Key或Token不正确.")
	}
	close(r.close)
	return nil
}
//防止任务没执行完就失效了
func (r *RedisLock) AddExpire() {
	go func() {
		tickDuration := r.ExpireMs - r.ExpireMs / 10 * 9
		if tickDuration == r.ExpireMs {
			return
		}
		tick := time.Tick( time.Duration(tickDuration) * time.Millisecond)
		c := r.pool.Get()
		defer c.Close()
		for {
			select {
			case <-tick:
				reply, err := redis.String(c.Do("SET", r.Key, r.KeyToken, "NX", "PX", r.ExpireMs/3))
				if err != nil {
					return
				}
				if reply != "OK" {
					return
				}
			case <-r.close:
				return
			}
		}
	}()
}

func (r *redisLock) BLock(key string, expireMs int, delayWaitMs int) (*RedisLock, error) {
	var result *RedisLock
	localLocalVar.lock(key).Lock()
	c := r.Pool.Get()
	channel := make(chan error, 1)
	defer func() {
		c.Close()
		close(channel)
		if result != nil && !result.Success {
			localLocalVar.unlLock(key)
		}
	}()

	timeClock := time.After(time.Millisecond * time.Duration(delayWaitMs))

	sleepTime := 50 * time.Millisecond

	var err error
	go func() {
		for i := 0; ; i++ {
			result, err = r.lock(key, expireMs)
			if err != nil {
				channel <- err
				return
			}
			if result.Success {
				channel <- nil
				return
			}
			select {
			case <-timeClock:
				channel <- errTimeOut
				return
			default:

			}
			sleepTime = sleepTime * (1<<i | 32)
			time.Sleep(sleepTime)
		}
	}()
	select {
		case err =<- channel:
			result.ExpireMs = expireMs
			return result, err
	}
}

func getSubscribeChanel(key string) string {
	return key + "unlock.channel"
}

var errTimeOut = errors.New("get lock time out")

func (r *redisLock) BLockWithSub(key string, expireMs int, delayWaitMs int) (*RedisLock, error) {
	localLocalVar.lock(key).Lock()
	c := r.Pool.Get()
	defer c.Close()
	timeClock := time.After(time.Millisecond * time.Duration(delayWaitMs))
	funcLock := func() (*RedisLock, chan error, error) {
		result, err := r.lock(key, expireMs)
		if err != nil {
			return result, nil, err
		}
		if result.Success {
			return result, nil, nil
		}
		err = c.Send("PSUBSCRIBE", getSubscribeChanel(key))
		if err != nil {
			return nil, nil, err
		}
		err = c.Flush()
		if err != nil {
			return nil, nil, err
		}
		channel := make(chan error, 1)
		go func() {
			_, err := redis.Values(c.Receive())
			if err != nil {
				channel <- err
				return
			}
			channel <- nil
		}()
		return nil, channel, err
	}

	for {
		result, channel, err := funcLock()
		if err != nil {
			return nil, err
		}

		select {
		case err = <-channel:
			break
		case err = <-timeClock:
			break
		}
		close(channel)
		if result.Success || err == nil {
			result.Success = true
			result.ExpireMs = expireMs
			return result, nil
		}
	}

}

const (
	multiLockScript = `
if #KEYS > 100 then
	return redis.error_reply("keys len is too long")
end
local lockResult = {}	
for i = 2,  #KEYS do
	if redis.call("SET", KEYS[i], ARGV[i], "NX", KEYS[1], ARGV[1]) ~= false then
		lockResult[i-1] =  KEYS[i]
	end
end
return lockResult
`

	multiUnLockScript = `
if #KEYS > 100 then
	return redis.error_reply("keys len is too long")
end
local delResult = {}
for i = 1,  #KEYS do
	local val = redis.call("GET",KEYS[i])
	if val ==  ARGV[i] then
		delResult[i] = KEYS[i]
	elseif (val == false) then
		delResult[i] = KEYS[i]
	end
end	
return delResult
`
)

func (r *redisLock) MultiLock(key []string, expireMs int) ([]*RedisLock, error) {

	c := r.Get()
	defer c.Close()
	args := make([]interface{}, len(key)*2+2)
	args[len(key)+1] = expireMs
	args[0] = "PX"
	keyMap := make(map[string]string)
	for i, v := range key {
		args[i+1] = v
		UUID, err := uuid.NewUUID()
		if err != nil {
			return nil, err
		}
		keyValue := UUID.String()

		args[2+len(key)+i] = keyValue
		keyMap[v] = keyValue
	}
	cmd := redis.NewScript(len(key)+1, multiLockScript)
	res, err := redis.Strings(cmd.Do(c, args...))
	if err == redis.ErrNil {
		return make([]*RedisLock, 0), nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]*RedisLock, 0)
	for _, v := range res {
		result = append(result, &RedisLock{Key: v, KeyToken: keyMap[v]})
	}

	return result, nil
}

func (r *redisLock) MultiUnLock(rsList []*RedisLock) ([]*RedisLock, error) {

	c := r.Get()
	defer c.Close()

	cmd := redis.NewScript(len(rsList), multiUnLockScript)
	args := make([]interface{}, len(rsList)*2)
	for i, v := range rsList {
		args[i] = v.Key
		args[len(rsList)+i] = v.KeyToken
	}
	res, err := redis.Strings(cmd.Do(c, args...))
	if err == redis.ErrNil {
		return make([]*RedisLock, 0), nil
	}
	if err != nil {
		return nil, err
	}

	result := make([]*RedisLock, 0)
	for _, v := range res {
		result = append(result, &RedisLock{Key: v})
	}

	return result, nil
}

//转换byte 为string， 适用于大字符转换，没有额外的内存拷贝
func ConvertByteToStr(bytes []byte) string {
	return *(*string)(unsafe.Pointer(&bytes))
}

//转换string 为byte， 适用于大字符转换，没有额外的内存拷贝
func ConvertStrToByte(str string) []byte {
	var b []byte
	*(*int)(unsafe.Pointer(uintptr(unsafe.Pointer(&b)) + 2*unsafe.Sizeof(&b))) = len(str) // 修改容量为长度
	*(*string)(unsafe.Pointer(&b)) = str                                                  // 把s的地址付给b
	return b
}
