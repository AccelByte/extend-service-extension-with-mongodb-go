// Copyright (c) 2023 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package service

import (
	"context"
	pb "extend-custom-guild-service/pkg/pb"
	"extend-custom-guild-service/pkg/storage"
	"fmt"

	"github.com/AccelByte/accelbyte-go-sdk/services-api/pkg/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MyServiceServerImpl struct {
	pb.UnimplementedServiceServer
	tokenRepo   repository.TokenRepository
	configRepo  repository.ConfigRepository
	refreshRepo repository.RefreshTokenRepository
	storage     storage.Storage
}

func NewMyServiceServer(
	tokenRepo repository.TokenRepository,
	configRepo repository.ConfigRepository,
	refreshRepo repository.RefreshTokenRepository,
	storage storage.Storage,
) *MyServiceServerImpl {
	return &MyServiceServerImpl{
		tokenRepo:   tokenRepo,
		configRepo:  configRepo,
		refreshRepo: refreshRepo,
		storage:     storage,
	}
}

func (g MyServiceServerImpl) CreateOrUpdateGuildProgress(
	ctx context.Context, req *pb.CreateOrUpdateGuildProgressRequest,
) (*pb.CreateOrUpdateGuildProgressResponse, error) {
	// Create or update guild progress in CloudSave
	// This assumes we're storing guild progress as a JSON object
	namespace := req.Namespace
	guildProgressKey := fmt.Sprintf("guildProgress_%s", req.GuildProgress.GuildId)
	guildProgressValue := req.GuildProgress
	guildProgress, err := g.storage.SaveGuildProgress(ctx, namespace, guildProgressKey, guildProgressValue)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error updating guild progress: %v", err)
	}

	// Return the updated guild progress
	return &pb.CreateOrUpdateGuildProgressResponse{GuildProgress: guildProgress}, nil
}

func (g MyServiceServerImpl) GetGuildProgress(
	ctx context.Context, req *pb.GetGuildProgressRequest,
) (*pb.GetGuildProgressResponse, error) {
	// Get guild progress in CloudSave
	namespace := req.Namespace
	guildProgressKey := fmt.Sprintf("guildProgress_%s", req.GuildId)

	guildProgress, err := g.storage.GetGuildProgress(ctx, namespace, guildProgressKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error getting guild progress: %v", err)
	}

	return &pb.GetGuildProgressResponse{
		GuildProgress: guildProgress,
	}, nil
}
