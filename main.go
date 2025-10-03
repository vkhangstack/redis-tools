package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	uuid "github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

var (
	Addr     = "localhost:6379"
	DB       = 0
	Password = ""
)

func delKeys() {
	ctx := context.Background()
	envFile, _ := godotenv.Read(".env")

	// rdb := redis.NewClient(&redis.Options{
	// 	Addr:     Addr, // Change if needed
	// 	DB:       DB,   // Use your desired DB
	// 	Password: Password,
	// })
	ipStr := envFile["REDIS_HOST"]
	fmt.Println("ipStr", ipStr)
	// Split the addresses by comma
	addrs := strings.Split(ipStr, ",")
	// fmt.Println("addrs", addrs)
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addrs, // Example cluster addresses
		// Add Password if your cluster requires it
		// Password: "ymvV7p1bE761",
		Password: envFile["REDIS_PASSWORD"],
	})

	// Ping the cluster to ensure connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Could not connect to Redis Cluster: %v", err)
	}
	fmt.Println("Connected to Redis Cluster!")

	// --- STEP 1: Populate some test data (optional, for demonstration) ---
	fmt.Println("Populating test data...")
	for i := 0; i < 50; i++ { // Create 50 keys, some with a prefix, some without
		key := fmt.Sprintf("myprefix:testkey:%d", i)
		if i%5 == 0 { // Some keys without the prefix
			key = fmt.Sprintf("anotherkey:%d", i)
		}
		err = rdb.Set(ctx, key, fmt.Sprintf("value%d", i), 0).Err()
		if err != nil {
			log.Printf("Error setting key %s: %v", key, err)
		}
	}
	fmt.Println("Test data populated.")
	time.Sleep(1 * time.Second) // Give Redis a moment

	// --- STEP 2: Scan and Delete All Keys ---
	fmt.Println("\nStarting to scan and delete ALL keys...")

	var totalKeysDeleted int64
	var mu sync.Mutex // Mutex to protect totalKeysDeleted

	// For Redis Cluster, you must iterate over each master node
	err = rdb.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
		addr := client.Options().Addr
		fmt.Printf("Scanning node: %s\n", addr)

		var cursor uint64           // Cursor for the current node
		keysBatchSize := int64(100) // Process keys in batches

		for {
			var keys []string
			var scanErr error

			// Scan for ALL keys ("*") on the current node
			keys, cursor, scanErr = client.Scan(ctx, cursor, "*", keysBatchSize).Result()
			if scanErr != nil {
				log.Printf("Error scanning keys on node %s: %v", addr, scanErr)
				return scanErr // Return error to stop scanning this node
			}

			if len(keys) > 0 {
				// Use UNLINK for non-blocking deletion
				for _, key := range keys {

					deleted, unlinkErr := client.Unlink(ctx, key).Result()
					if unlinkErr != nil {
						log.Printf("Error unlinking keys on node %s: %v", addr, unlinkErr)
						// Decide if you want to continue or stop on error
						// For robust deletion, you might log and continue.
					} else {
						mu.Lock()
						totalKeysDeleted += deleted
						mu.Unlock()
						fmt.Printf("Unlinked %d keys from node %s. Total deleted so far: %d\n", deleted, addr, totalKeysDeleted)
					}
				}

			}

			if cursor == 0 { // No more keys to scan on this node
				fmt.Printf("Finished scanning node: %s\n", addr)
				break
			}
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Error during cluster-wide scan and delete: %v", err)
	}

	fmt.Printf("\nFinished deleting all keys from Redis Cluster. Total keys unlinked: %d\n", totalKeysDeleted)

	// --- STEP 3: Verify (optional) ---
	fmt.Println("\nVerifying keyspace after deletion...")
	time.Sleep(1 * time.Second) // Give Redis a moment for async UNLINK

	err = rdb.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
		info, err := client.Info(ctx, "keyspace").Result()
		if err != nil {
			log.Printf("Error getting keyspace info from node %s: %v", client.Options().Addr, err)
			return err
		}
		fmt.Printf("Keyspace Info for node %s:\n%s\n", client.Options().Addr, info)
		return nil
	})
	if err != nil {
		log.Printf("Error during keyspace verification: %v", err)
	}
	// var cursor uint64
	// var batchSize int64 = 100

	// for {
	// 	keys, nextCursor, err := rdb.Scan(ctx, cursor, "*", batchSize).Result()
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	if len(keys) > 0 {
	// 		// Delete keys
	// 		_, err = rdb.Del(ctx, keys...).Result()
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		fmt.Printf("Deleted keys: %v\n", keys)
	// 	}

	// 	cursor = nextCursor
	// 	if cursor == 0 {
	// 		break
	// 	}
	// }

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
func setKey(key string, value string) {
	ctx := context.Background()
	envFile, _ := godotenv.Read(".env")

	// rdb := redis.NewClient(&redis.Options{
	// 	Addr:     Addr, // Change if needed
	// 	DB:       DB,   // Use your desired DB
	// 	Password: Password,
	// })
	ipStr := envFile["REDIS_HOST"]
	fmt.Println("ipStr", ipStr)
	// Split the addresses by comma
	addrs := strings.Split(ipStr, ",")
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addrs, // Example cluster addresses
		// Add Password if your cluster requires it
		// Password: "ymvV7p1bE761",
		Password: envFile["REDIS_PASSWORD"],
	})
	fmt.Println("setKey")

	err := rdb.Set(ctx, key, value, 0).Err()
	if err != nil {
		panic(err)
	}
}
func delKey(key string) {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     Addr, // Change if needed
		DB:       DB,   // Use your desired DB
		Password: Password,
	})
	fmt.Println("DelKey")

	err := rdb.Del(ctx, key).Err()
	if err != nil {
		panic(err)
	}
}
func getKey(key string) string {
	ctx := context.Background()
	envFile, _ := godotenv.Read(".env")

	// rdb := redis.NewClient(&redis.Options{
	// 	Addr:     Addr, // Change if needed
	// 	DB:       DB,   // Use your desired DB
	// 	Password: Password,
	// })
	ipStr := envFile["REDIS_HOST"]
	fmt.Println("ipStr", ipStr)
	// Split the addresses by comma
	addrs := strings.Split(ipStr, ",")
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addrs, // Example cluster addresses
		// Add Password if your cluster requires it
		Password: envFile["REDIS_PASSWORD"],
	})
	fmt.Println("getKey")

	err := rdb.Get(ctx, key).Err()
	if err != nil {
		panic(err)
	}
	value := rdb.Get(ctx, key).Val()
	return value
}
func main() {
	// delKey("{inventory_award}:666173941106335744")

	// delKeys()
}
