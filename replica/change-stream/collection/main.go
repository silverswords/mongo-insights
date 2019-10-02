package main

import (
	"context"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type change struct {
	Name  string `bson:"name,omitempty"`
	Count int    `bson:"count,omitempty"`
}

func main() {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://mongo-rs-n1:27017, mongo-rs-n2:27017, mongo-rs-n3:27017"))

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

	var wg sync.WaitGroup
	wg.Add(2)

	collection := client.Database("simple").Collection("change")

	go func() {
		opts := options.ChangeStream()
		opts.SetFullDocument("updateLookup")

		cs, err := collection.Watch(context.Background(), mongo.Pipeline{}, opts)

		if err != nil {
			log.Fatal("Watch Stream ", err)
		}
		defer cs.Close(context.Background())

		for cs.Next(context.Background()) {
			var ele bson.M

			if err := cs.Decode(&ele); err != nil {
				log.Println("Watch Stream Decode error ", err)
				continue
			}
			log.Println("Watch Stream received:", ele)
		}

		log.Println("Watching Stream quit with error:", cs.Err())

		wg.Done()
	}()

	go func() {
		opts := options.ChangeStream()
		opts.SetFullDocument("updateLookup")

		cs, err := collection.Watch(context.Background(), mongo.Pipeline{
			{{"$match", bson.D{{"fullDocument.name", "trigger"}}}},
		}, opts)

		if err != nil {
			log.Fatal("Watch Stream [Match] ", err)
		}
		defer cs.Close(context.Background())

		for cs.Next(context.Background()) {
			var ele bson.M

			if err := cs.Decode(&ele); err != nil {
				log.Println("Watch Stream [[Match]] Decode error ", err)
				continue
			}
			log.Println("Watch Stream [[Match]] received:", ele)
		}

		log.Println("Watching Stream [[Match]] quit with error:", cs.Err())

		wg.Done()
	}()

	wg.Wait()
}
