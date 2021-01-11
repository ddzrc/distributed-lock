redis-lock
compare to other distributed lock, it has the following advantages
1. introduce redis PSUBSCRIBE, not need to cycle call redis, to acquire the lock has been unlock
2. add the local memory lock to decrease redis qps
3. add AddExpire func, in case task is not finished, and the lock is invalid. 