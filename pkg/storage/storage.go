// Copyright (c) 2023 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package storage

import (
	"context"
	"encoding/json"
	pb "extend-custom-guild-service/pkg/pb"

	"github.com/AccelByte/accelbyte-go-sdk/cloudsave-sdk/pkg/cloudsaveclientmodels"

	"github.com/AccelByte/accelbyte-go-sdk/cloudsave-sdk/pkg/cloudsaveclient/admin_game_record"
	"github.com/AccelByte/accelbyte-go-sdk/services-api/pkg/service/cloudsave"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Storage interface {
	GetGuildProgress(ctx context.Context, namespace string, key string) (*pb.GuildProgress, error)
	SaveGuildProgress(ctx context.Context, namespace string, key string, value *pb.GuildProgress) (*pb.GuildProgress, error)
}

type CloudsaveStorage struct {
	csStorage *cloudsave.AdminGameRecordService
}

func NewCloudSaveStorage(csStorage *cloudsave.AdminGameRecordService) *CloudsaveStorage {
	return &CloudsaveStorage{
		csStorage: csStorage,
	}
}

func (c *CloudsaveStorage) SaveGuildProgress(ctx context.Context, namespace string, key string, value *pb.GuildProgress) (*pb.GuildProgress, error) {
	input := &admin_game_record.AdminPostGameRecordHandlerV1Params{
		Body:      value,
		Key:       key,
		Namespace: namespace,
		Context:   ctx,
	}
	response, err := c.csStorage.AdminPostGameRecordHandlerV1Short(input)
	if err != nil {
		return nil, err
	}

	guildProgress, err := parseResponseToGuildProgress(response)
	if err != nil {
		return nil, err
	}

	return guildProgress, nil
}

func (c *CloudsaveStorage) GetGuildProgress(ctx context.Context, namespace string, key string) (*pb.GuildProgress, error) {
	input := &admin_game_record.AdminGetGameRecordHandlerV1Params{
		Key:       key,
		Namespace: namespace,
		Context:   ctx,
	}
	response, err := c.csStorage.AdminGetGameRecordHandlerV1Short(input)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error getting guild progress: %v", err)
	}

	guildProgress, err := parseResponseToGuildProgress(response)
	if err != nil {
		return nil, err
	}

	return guildProgress, nil
}

func parseResponseToGuildProgress(response *cloudsaveclientmodels.ModelsGameRecordAdminResponse) (*pb.GuildProgress, error) {
	// Convert the response value to a JSON string
	valueJSON, err := json.Marshal(response.Value)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error marshalling value into JSON: %v", err)
	}

	// Unmarshal the JSON string into a pb.GuildProgress
	var guildProgress pb.GuildProgress
	err = json.Unmarshal(valueJSON, &guildProgress)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error unmarshalling value into GuildProgress: %v", err)
	}

	return &guildProgress, nil
}
