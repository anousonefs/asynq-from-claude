package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	pubnubgo "github.com/pubnub/go/v7"
)

var _ Pubnub = (*pubnub)(nil)

type PubNubConfig struct {
	PublishKey, SubscribeKey, SecretKey, UUIDKey, UUIDSubKey string
}

func NewPubnub(pnCfg *PubNubConfig) (Pubnub, error) {
	if pnCfg == nil {
		return nil, fmt.Errorf("[NewPubnub] pnCfg: must not be nil")
	}

	cfg := pubnubgo.NewConfigWithUserId(pubnubgo.UserId(pnCfg.UUIDKey))
	cfg.PublishKey = pnCfg.PublishKey
	cfg.SubscribeKey = pnCfg.SubscribeKey
	cfg.SecretKey = pnCfg.SecretKey

	return &pubnub{
		pn:         pubnubgo.NewPubNub(cfg),
		uuidSubKey: pnCfg.UUIDSubKey,
	}, nil
}

type Pubnub interface {
	Publish(ctx context.Context, uuid string, messagePayload any) (string, error)
	GenGrantToken(ctx context.Context) (string, error)
}

type pubnub struct {
	pn         *pubnubgo.PubNub
	uuidSubKey string
}

func (p *pubnub) Publish(ctx context.Context, uuid string, messagePayload any) (string, error) {
	messageJSON, err := setPrepareMessage(messagePayload)
	if err != nil {
		return "", err
	}

	// Add channel to message
	channel := fmt.Sprintf("channel-%s", uuid)

	// Publish the message to a channel
	publish := p.pn.Publish()
	publish.Channel(channel).Message(string(messageJSON))
	resp, _, err := publish.Execute()
	if err != nil {
		return "", err
	}

	s := strconv.FormatInt(resp.Timestamp, 10)
	return s, nil
}

func (p *pubnub) GenGrantToken(ctx context.Context) (string, error) {
	grantToken := p.pn.GrantTokenWithContext(ctx)
	permissions := map[string]pubnubgo.ChannelPermissions{
		"^channel-[A-Za-z0-9]*$": {
			Read: true,
		},
	}

	token, _, err := grantToken.TTL(60).AuthorizedUUID(p.uuidSubKey).ChannelsPattern(permissions).Execute()
	if err != nil {
		return "", err
	}

	return token.Data.Token, nil
}

// setPrepareMessage is a function to format message to JSON
func setPrepareMessage(messagePayload any) (string, error) {
	messageJSON, err := json.Marshal(messagePayload)
	if err != nil {
		return "", err
	}

	return string(messageJSON), nil
}
