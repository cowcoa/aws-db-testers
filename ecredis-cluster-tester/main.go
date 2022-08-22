package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	goredis "github.com/go-redis/redis/v8"
)

// The original author is Tang Jian, I just copied and modified his code. Thank you Tang Jian!
// https://aws.amazon.com/cn/blogs/china/all-roads-lead-to-rome-use-go-redis-to-connect-amazon-elasticache-for-redis-cluster/
func main() {
	// Get parameters
	endpoint := flag.String("endpoint", "", "Configuration endpoint(hostname:port)")
	operation := flag.String("operation", "w", "Run tester in 'w' or 'r' mode")
	password := flag.String("password", "", "Cluster credential")

	if len(os.Args) != 4 {
		fmt.Println("Continuously read and write redis cluster and output the results")
		fmt.Println()
		flag.Usage()
		fmt.Println()
		fmt.Println("Example:")
		fmt.Printf("%s --endpoint=clustercfg.cdkgoexample-rediscluster.bcvgti.apne1.cache.amazonaws.com:6379 --operation=w --password=mysecret \n\n", os.Args[0])
		os.Exit(1)
	}
	flag.Parse()

	if *operation != "w" && *operation != "r" {
		fmt.Println()
		fmt.Println("Invalid 'operation' parameter, valid values are 'w' or 'r'")
		fmt.Println()
		os.Exit(1)
	}

	// Redis client
	var ctx = context.Background()
	rdb := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:    []string{*endpoint},
		Password: *password, //密码
		//连接池容量及闲置连接数量
		PoolSize:     10, // 连接池最大socket连接数，默认为4倍CPU数， 4 * runtime.NumCPU
		MinIdleConns: 10, //在启动阶段创建指定数量的Idle连接，并长期维持idle状态的连接数不少于指定数量；。

		//超时
		DialTimeout:  5 * time.Second, //连接建立超时时间，默认5秒。
		ReadTimeout:  3 * time.Second, //读超时，默认3秒， -1表示取消读超时
		WriteTimeout: 3 * time.Second, //写超时，默认等于读超时
		PoolTimeout:  4 * time.Second, //当所有连接都处在繁忙状态时，客户端等待可用连接的最大等待时长，默认为读超时+1秒。

		//闲置连接检查包括IdleTimeout，MaxConnAge
		IdleCheckFrequency: 60 * time.Second, //闲置连接检查的周期，默认为1分钟，-1表示不做周期性检查，只在客户端获取连接时对闲置连接进行处理。
		IdleTimeout:        5 * time.Minute,  //闲置超时，默认5分钟，-1表示取消闲置超时检查
		MaxConnAge:         0 * time.Second,  //连接存活时长，从创建开始计时，超过指定时长则关闭连接，默认为0，即不关闭存活时长较长的连接

		//命令执行失败时的重试策略
		MaxRetries:      10,                     // 命令执行失败时，最多重试多少次，默认为0即不重试
		MinRetryBackoff: 8 * time.Millisecond,   //每次计算重试间隔时间的下限，默认8毫秒，-1表示取消间隔
		MaxRetryBackoff: 512 * time.Millisecond, //每次计算重试间隔时间的上限，默认512毫秒，-1表示取消间隔

		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},

		// ReadOnly = true，只择 Slave Node
		// ReadOnly = true 且 RouteByLatency = true 将从 slot 对应的 Master Node 和 Slave Node， 择策略为: 选择PING延迟最低的点
		// ReadOnly = true 且 RouteRandomly = true 将从 slot 对应的 Master Node 和 Slave Node 选择，选择策略为: 随机选择

		ReadOnly:       true,
		RouteRandomly:  true,
		RouteByLatency: true,
	})
	defer rdb.Close()

	// Do the test job
	if *operation == "w" {
		WriteTest(rdb, ctx)
	} else if *operation == "r" {
		ReadTest(rdb, ctx)
	}

	stats := rdb.PoolStats()
	fmt.Printf("Hits=%d Misses=%d Timeouts=%d TotalConns=%d IdleConns=%d StaleConns=%d\n",
		stats.Hits, stats.Misses, stats.Timeouts, stats.TotalConns, stats.IdleConns, stats.StaleConns)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func WriteTest(rdb *goredis.ClusterClient, ctx context.Context) {
	AllMaxRun := 6

	wg := sync.WaitGroup{}
	wg.Add(AllMaxRun)

	for i := 0; i < AllMaxRun; i++ {
		go func(wg *sync.WaitGroup, idx int) {
			defer wg.Done()

			for i := 0; i < 50000; i++ {
				key := "test-" + strconv.Itoa(i)
				val := RandStringBytes(6)
				_, err := rdb.Set(ctx, key, val, 0).Result()
				if err != nil {
					fmt.Printf("err : %s\n", err.Error())
				} else {
					fmt.Printf("%s Job-%d %s = %s-%d \n", time.Now().Format("2006-01-02 15:04:05"), idx, key, val, i)
				}
				time.Sleep(500 * time.Millisecond)
			}
		}(&wg, i)
	}

	wg.Wait()
}

func ReadTest(rdb *goredis.ClusterClient, ctx context.Context) {
	rdb.Set(ctx, "test-0", "value-0", 0)
	rdb.Set(ctx, "test-1", "value-1", 0)
	rdb.Set(ctx, "test-2", "value-2", 0)

	AllMaxRun := 6

	wg := sync.WaitGroup{}
	wg.Add(AllMaxRun)

	for i := 0; i < AllMaxRun; i++ {
		go func(wg *sync.WaitGroup, idx int) {
			defer wg.Done()

			for i := 0; i < 50000; i++ {
				key := "test-" + strconv.Itoa(i%3)
				val, err := rdb.Get(ctx, key).Result()
				if err == goredis.Nil {
					fmt.Println("job-" + strconv.Itoa(idx) + " " + key + " does not exist")
				} else if err != nil {
					fmt.Printf("err : %s\n", err.Error())
				} else {
					fmt.Printf("%s Job-%d %s = %s-%d \n", time.Now().Format("2006-01-02 15:04:05"), idx, key, val, i)
				}
				time.Sleep(500 * time.Millisecond)
			}
		}(&wg, i)
	}

	wg.Wait()
}
