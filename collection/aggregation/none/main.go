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

	opt := options.Aggregate()
	opt.SetAllowDiskUse(true)

	cursor, err := collection.Aggregate(context.Background(), mongo.Pipeline{}, opt)

	if err != nil {
		log.Fatal(err)
	}

	cursorCtx := context.Background()
	defer cursor.Close(cursorCtx)

	for cursor.Next(cursorCtx) {
		var s stock

		if err := cursor.Decode(&s); err != nil {
			log.Fatal(err)
		}

		log.Println(s)
	}
}
