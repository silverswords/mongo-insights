package main

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type stock struct {
	StockID string  `bson:"stockId,omitempty"`
	Price   float32 `bson:"price"`
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

	collection := client.Database("aggregation").Collection("stock")
	for i := 0; i < 10; i++ {
		s := stock{
			StockID: "Google",
			Price:   float32(10.0 * (i + 1)),
		}

		if _, err := collection.InsertOne(context.Background(), &s); err != nil {
			log.Fatal(err)
		}
	}

	for i := 0; i < 20; i++ {
		s := stock{
			StockID: "Apple",
			Price:   float32(20.0 * (i + 1)),
		}

		if _, err := collection.InsertOne(context.Background(), &s); err != nil {
			log.Fatal(err)
		}
	}
}
