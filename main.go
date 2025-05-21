package main

import (
	"context"
	"fmt"

	uuid "github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	Addr     = "localhost:6379"
	DB       = 4
	Password = ""
)

func delKeys() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     Addr, // Change if needed
		DB:       DB,   // Use your desired DB
		Password: Password,
	})

	var cursor uint64
	var batchSize int64 = 100

	for {
		keys, nextCursor, err := rdb.Scan(ctx, cursor, "*", batchSize).Result()
		if err != nil {
			panic(err)
		}

		if len(keys) > 0 {
			// Delete keys
			_, err = rdb.Del(ctx, keys...).Result()
			if err != nil {
				panic(err)
			}
			fmt.Printf("Deleted keys: %v\n", keys)
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	fmt.Println("All keys deleted.")
}
func setKeys() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     Addr, // Change if needed
		DB:       DB,   // Use your desired DB
		Password: Password,
	})
	fmt.Println("sss")

	var max = 1000000

	for i := 1; i <= max; i++ {
		id, _ := uuid.NewUUID()
		fmt.Println("id", id.String())
		err := rdb.Set(ctx, id.String(), "true", 0).Err()
		if err != nil {
			panic(err)
		}
	}
}
func main() {
	setKeys()
	delKeys()
}
