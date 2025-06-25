package main

import (
	"log"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func connectMongo50() (*mongo.Client, error) {
	uri := "mongodb://localhost:27015"
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		log.Println("Error connecting:", err)
		return nil, err
	}
	return client, nil
}

func connectMongo60() (*mongo.Client, error) {
	uri := "mongodb://localhost:27016"
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		log.Println("Error connecting:", err)
		return nil, err
	}
	return client, nil
}

func connectMongo70() (*mongo.Client, error) {
	uri := "mongodb://localhost:27017"
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		log.Println("Error connecting:", err)
		return nil, err
	}
	return client, nil
}

func connectMongo80() (*mongo.Client, error) {
	uri := "mongodb://localhost:27018"
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		log.Println("Error connecting:", err)
		return nil, err
	}
	return client, nil
}
