// Copyright (c) 2023 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package storage

import (
	"context"
	"fmt"
	"time"

	pb "extend-custom-guild-service/pkg/pb"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Storage interface {
	GetGuildProgress(ctx context.Context, namespace string, key string) (*pb.GuildProgress, error)
	SaveGuildProgress(ctx context.Context, namespace string, key string, value *pb.GuildProgress) (*pb.GuildProgress, error)
}

// GuildProgressDocument represents the MongoDB document structure
type GuildProgressDocument struct {
	Key        string           `bson:"key"`
	Namespace  string           `bson:"namespace"`
	GuildID    string           `bson:"guild_id"`
	Objectives map[string]int32 `bson:"objectives"`
	CreatedAt  time.Time        `bson:"created_at"`
	UpdatedAt  time.Time        `bson:"updated_at"`
}

type MongoDBStorage struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
}

func NewMongoDBStorage(connectionString, databaseName string) (*MongoDBStorage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connectionString))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Test the connection
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	database := client.Database(databaseName)
	collection := database.Collection("guild_progress")

	// Create indexes for better performance
	_, err = collection.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "namespace", Value: 1},
				{Key: "key", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
	})
	if err != nil {
		// Log the error but don't fail the initialization
		fmt.Printf("Warning: failed to create indexes: %v\n", err)
	}

	return &MongoDBStorage{
		client:     client,
		database:   database,
		collection: collection,
	}, nil
}

func (m *MongoDBStorage) SaveGuildProgress(ctx context.Context, namespace string, key string, value *pb.GuildProgress) (*pb.GuildProgress, error) {
	now := time.Now()

	// Use upsert to create or update
	filter := bson.M{
		"namespace": namespace,
		"key":       key,
	}

	update := bson.M{
		"$set": bson.M{
			"guild_id":   value.GuildId,
			"objectives": value.Objectives,
			"updated_at": now,
		},
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}

	opts := options.Update().SetUpsert(true)
	_, err := m.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error saving guild progress to MongoDB: %v", err)
	}

	// Return the value that was saved
	return value, nil
}

func (m *MongoDBStorage) GetGuildProgress(ctx context.Context, namespace string, key string) (*pb.GuildProgress, error) {
	filter := bson.M{
		"namespace": namespace,
		"key":       key,
	}

	var doc GuildProgressDocument
	err := m.collection.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, status.Errorf(codes.NotFound, "Guild progress not found for namespace: %s, key: %s", namespace, key)
		}
		return nil, status.Errorf(codes.Internal, "Error getting guild progress from MongoDB: %v", err)
	}

	return m.documentToGuildProgress(&doc), nil
}

func (m *MongoDBStorage) documentToGuildProgress(doc *GuildProgressDocument) *pb.GuildProgress {
	return &pb.GuildProgress{
		GuildId:    doc.GuildID,
		Namespace:  doc.Namespace,
		Objectives: doc.Objectives,
	}
}

// Close closes the MongoDB connection
func (m *MongoDBStorage) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}
