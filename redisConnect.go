// Task for you
// Read about redis,
// write a code in go to connect to redis,
// Producer: generate random values of age and push it to redis
// Consumer: consume values from redis and find average and total age.
// Match the values from producer and consumer.
// time.Sleep(1 * time.Second) // Wait for 1 second before consuming the next item
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	. "github.com/remiges-tech/logharbour/logharbour"
)

var ctx = context.Background()
var totalAgeProduce, totalCharProduce = 0, 0 
var totalAgeConsume, totalCharConsume = 0, 0
var mutexProduceConsumer = &sync.Mutex{}

func main() {
	err := godotenv.Load("config.env")
	if err != nil {
		slog.Error("Error loading .env file:", err)
	}
	
	// Access the environment variables
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbPass := os.Getenv("DB_PASS")
	dbIndex, _ := strconv.Atoi(os.Getenv("DB_INDEX"))

	// Connection
	client := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", dbHost, dbPort),
		Password: dbPass,
		DB: dbIndex,
	})

	logFile, err := os.OpenFile("log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		slog.Error("Error in Opening Log file:", err)
	}
	err = logFile.Truncate(0)
	if err != nil {
		slog.Error("Error in deleting old content:", err)
	}

	fallbackWriter := NewFallbackWriter(logFile, os.Stdout)
	lctx := NewLoggerContext(Info)
	logger := NewLoggerWithFallback(lctx, "MyApp", fallbackWriter)
	logger = logger.WithModule("Module 1").
		WithWho("Vrutik Savla").
		WithStatus(Success).
		WithRemoteIP("")	

	var producerConsumerWg sync.WaitGroup
	producerConsumerWg.Add(2)
	go func() {
		defer producerConsumerWg.Done()
		createProducers(&producerConsumerWg, client, 10, 2, logger)
	}()
	go func() {
		defer producerConsumerWg.Done()
		consumer(&producerConsumerWg, client, "people", logger)
	}()
	producerConsumerWg.Wait()
		
	logger.LogActivity("Checksum Producer", map[string]any{"Total Characters Produced": totalCharProduce})
	logger.LogActivity("Checksum Producer", map[string]any{"Total Age Produced": totalAgeProduce})
	logger.LogActivity("Checksum Consumer", map[string]any{"Total Age Consumed": totalAgeConsume})
	logger.LogActivity("Checksum Producer", map[string]any{"Total Age Consumed": totalAgeConsume})

	// fmt.Printf("Total Characters Produced: %d, Total Age Produced: %d", totalCharProduce, totalAgeProduce)
	// fmt.Printf("\nTotal Characters Consumed: %d, Total Age Consumed: %d", totalCharConsume, totalAgeConsume)

	if totalAgeProduce == totalAgeConsume && totalCharProduce == totalCharConsume {
		logger.Log("Checksum Matched!")
		// fmt.Print("\nChecksum Matched!")
	} else {
		logger.Log("Checksum Didn't Matched!")
		// fmt.Print("\nChecksum Didn't Matched")
	}

	logFile.Close()
	client.Close()
}

// producer: Producer function produces random names & age as string & integer respectively for provided count & then it pushes it in redis database as a list data structure
func producer(client *redis.Client, count int, logger *Logger) {
	for i:=0; i<count; i++ {
		name := generateRandomName(rand.Intn(200)) //Generate random name of 0 to 200 characters
		age := rand.Intn(100) //Generate rantom age between 0 to 100
		
		// Adding total age & total characters that are being produced & preventing race conditions
		mutexProduceConsumer.Lock()
        totalAgeProduce += age
        totalCharProduce += len(name)
		mutexProduceConsumer.Unlock()

		logger.LogActivity("Pushed", map[string]any{name: age})
		// fmt.Printf("\nPushed:\n%s:%d\n", name, age)
		
		err := client.LPush(ctx, "people", fmt.Sprintf("%s:%d", name, age)).Err() //Push data into DB
		if err != nil {
			logger.Error(err)
			// slog.Error("Error pushing to Redis:", err)
		}
	}
}

// createProducers: This function takes 3 arguments (Redis client, total number of producers & total producers per thread), it calls producer function to then produce via multi threading using go routines.
func createProducers(producerWg *sync.WaitGroup, client *redis.Client, totalProducers, producersPerThread int, logger *Logger) {
	// var wg sync.WaitGroup //A WaitGroup waits for a collection of goroutines to finish. The main goroutine calls Add to set the number of goroutines to wait for. Then each of the goroutines runs and calls Done when finished. At the same time, Wait can be used to block until all goroutines have finished.

	threads := totalProducers / producersPerThread
	for i := 0; i < threads; i++ {
		producerWg.Add(1)

		// go routine for multi-threading
		go func() {
			defer producerWg.Done()
			producer(client, producersPerThread, logger)
		}()
	}

	remaining := totalProducers % producersPerThread
	if remaining > 0 {
		producerWg.Add(1)
		// go routine for multi-threading
		go func() {
			defer producerWg.Done()
			producer(client, remaining, logger)
		}()
	}
}

// consumer: Consumer function pops the value from redis database & also calcuates total age count & average of age while consuming in each step 
func consumer(consumerWg *sync.WaitGroup, client *redis.Client, key string, logger *Logger) {
	time.Sleep(3 * time.Millisecond)
	for {
		val, err := client.RPop(context.Background(), key).Result()

		if err == redis.Nil {
			// When the key doesn't exist (empty list)
			break
		} else if err != nil {
			logger.Error(err)
			// slog.Error("Error in popping the result:", err)
			continue // Continue processing other elements
		}

		data := splitValue(val, ":")
		age, err := strconv.Atoi(data[1])
		if err != nil {
			logger.Error(err)
			// slog.Error("Error in parsing integer:", err)
			continue // Continue processing other elements
		}

		totalAgeConsume += age
		totalCharConsume += len(data[0])

		logger.LogActivity("Popped", map[string]any{data[0]: age})
		// fmt.Printf("\nPopped:\n%s:%d\n", data[0], age)
	}
}


// generateRandomName: This function generates random string of length passed as an argument.
func generateRandomName(length int) string {
	const charSet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	name := make([]byte, length) //Make empty array for provided length
	for i := range name {
		name[i] = charSet[rand.Intn(len(charSet))] //Generate random string for provided length
	}
	return string(name)
}

// splitValue: This function takes a string & separator as arguments & separates the string with provided separator & return string elements in form of array
func splitValue(value, separator string) []string {
	return strings.Split(value, separator)
}