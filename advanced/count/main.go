package main

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func now() time.Time {
	return time.Now()
}

func executionDuration(label string, start time.Time) {
	end := time.Now()

	duration := float64(1.0*(end.UnixNano()-start.UnixNano())) / float64(time.Second)
	log.Printf("[%s] Execution time %f\n", label, duration)
}

func countGoogle(collection *mongo.Collection) {
	defer executionDuration("mongo.advanced.Count.GOOGL", now())
	cnt, err := collection.CountDocuments(context.Background(), bson.D{{"code", "GOOGL"}})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Count(GOOGL) = ", cnt)
}

func countAll(collection *mongo.Collection) {
	defer executionDuration("mongo.advanced.Count.EMPTY", now())
	total, err := collection.CountDocuments(context.Background(), bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Count() = ", total)
}

func countAllByCommand(db *mongo.Database) {
	defer executionDuration("mongo.advanced.RunCommand.Count", now())
	result := db.RunCommand(context.Background(), bson.D{{"count", "nasdaq"}})
	if err := result.Err(); err != nil {
		log.Fatal(err)
	}

	raw, _ := result.DecodeBytes()
	log.Println("runCommand(count) = ", raw.String())
}

func countNonExists(collection *mongo.Collection) {
	defer executionDuration("mongo.advanced.Count.NONEXISTS", now())
	total, err := collection.CountDocuments(context.Background(), bson.D{{"code", "fengyfei"}})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Count(fengyfei) = ", total)
}

func main() {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://root:single@localhost:27017"))

	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	stocks := client.Database("stocks")
	collection := stocks.Collection("nasdaq")

	countGoogle(collection)

	countAll(collection)

	countAllByCommand(stocks)

	countNonExists(collection)
}
