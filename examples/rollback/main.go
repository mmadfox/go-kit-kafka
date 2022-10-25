package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-kit/kit/endpoint"
	kafka "github.com/mmadfox/go-kit-kafka"
)

type AccountUseCase interface {
	UpdateAccount(ctx context.Context, deviceID int, userID int) error
	RollbackAccount(ctx context.Context, deviceID int, userID int) error
}

type accountUseCase struct {
	eventPublisher kafka.EventPublisher
}

func (uc accountUseCase) UpdateAccount(ctx context.Context, deviceID int, userID int) error {
	fmt.Println("UpdateAccount")

	if deviceID == 2 {
		return errors.New("err")
	}

	return nil
}

func (uc accountUseCase) RollbackAccount(ctx context.Context, deviceID int, userID int) error {
	fmt.Println("RollbackAccount")
	return nil
}

func main() {
	broker := kafka.NewBroker()
	svc := accountUseCase{}

	updateAccountHandler := kafka.NewConsumer(
		makeUpdateAccountEndpoint(svc),
		decodeUpdateAccount,
	)

	rollbackAccountHandler := kafka.NewConsumer(
		makeRollbackAccountEndpoint(svc),
		decodeRollbackAccount,
	)

	channelName := kafka.Topic("account.svc")
	broker.NewChannel(channelName).
		Handler(updateAccountHandler).
		RollbackHandler(rollbackAccountHandler)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		payload, _ := json.Marshal(accountUpdatedEvent{
			DeviceID: i,
			UserName: i + 1,
			UserID:   i + 2,
		})
		err := broker.HandleMessage(ctx, &kafka.Message{
			Topic: channelName,
			Value: payload,
		})
		_ = err
	}
}

type accountUpdatedEvent struct {
	DeviceID int `json:"deviceId"`
	UserID   int `json:"userId"`
	UserName int `json:"userName"`
}

func (e accountUpdatedEvent) String() string {
	return fmt.Sprintf("accountUpdatedEvent{%d, %d, %d}",
		e.DeviceID, e.UserID, e.UserName)
}

func decodeUpdateAccount(_ context.Context, msg *kafka.Message) (request any, err error) {
	event := accountUpdatedEvent{}
	if err = json.Unmarshal(msg.Value, &event); err != nil {
		return nil, err
	}
	return event, nil
}

func decodeRollbackAccount(_ context.Context, msg *kafka.Message) (request any, err error) {
	event := accountUpdatedEvent{}
	if err = json.Unmarshal(msg.Value, &event); err != nil {
		return nil, err
	}
	return event, nil
}

func makeUpdateAccountEndpoint(uc AccountUseCase) endpoint.Endpoint {
	return func(ctx context.Context, request any) (response any, err error) {
		event := request.(accountUpdatedEvent)
		err = uc.UpdateAccount(ctx, event.DeviceID, event.UserID)
		return nil, err
	}
}

func makeRollbackAccountEndpoint(uc AccountUseCase) endpoint.Endpoint {
	return func(ctx context.Context, request any) (response any, err error) {
		event := request.(accountUpdatedEvent)
		err = uc.RollbackAccount(ctx, event.DeviceID, event.UserID)
		return nil, err
	}
}
