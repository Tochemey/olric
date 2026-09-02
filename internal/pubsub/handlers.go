/*
 * Copyright 2018-2024 Burak Sezer
 * Copyright 2025-2026 Arsene Tochemey Gandote
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pubsub

import (
	"context"

	"github.com/tidwall/redcon"

	"github.com/tochemey/olric/internal/protocol"
)

func (s *Service) subscribeCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	subscribeCmd, err := protocol.ParseSubscribeCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	for _, channel := range subscribeCmd.Channels {
		s.pubsub.Subscribe(conn, channel)
		CurrentSubscribers.Increase(1)
		SubscribersTotal.Increase(1)
	}
}

// publishToCluster delivers message to the subscribers of channel on every
// cluster member and returns the total number of receivers. Local subscribers
// are served in-process; remote members receive the message over
// PUBLISH.INTERNAL.
func (s *Service) publishToCluster(ctx context.Context, channel, message string) (int64, error) {
	// A canceled context must not deliver anywhere, local subscribers
	// included — the old client-side Publish path rejected it up front.
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	// Local subscribers are served first and never wait on a remote member: a
	// member that is still starting can hold or reject the internal publish,
	// and that must neither delay nor lose the event locally. Every remote
	// member is then attempted, and the first failure is reported only after
	// the last attempt, so one unreachable member does not cost the others
	// their copy.
	total := int64(s.pubsub.Publish(channel, message))
	PublishedTotal.Increase(total)

	var firstErr error
	for _, member := range s.rt.Discovery().GetMembers() {
		if member.CompareByID(s.rt.This()) {
			continue
		}

		pi := protocol.NewPublishInternal(channel, message).Command(ctx)
		rc := s.client.Get(member.String())
		if err := rc.Process(ctx, pi); err != nil {
			if firstErr == nil {
				firstErr = err
			}

			continue
		}

		pcount, err := pi.Result()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}

			continue
		}

		total += pcount
		PublishedTotal.Increase(pcount)
	}

	return total, firstErr
}

func (s *Service) publishCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	publishCmd, err := protocol.ParsePublishCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	total, err := s.publishToCluster(s.ctx, publishCmd.Channel, publishCmd.Message)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	conn.WriteInt(int(total))
}

func (s *Service) publishInternalCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	publishInternalCmd, err := protocol.ParsePublishInternalCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	count := s.pubsub.Publish(publishInternalCmd.Channel, publishInternalCmd.Message)
	conn.WriteInt(count)
}

func (s *Service) psubscribeCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	psubscribeCmd, err := protocol.ParsePSubscribeCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	for _, pattern := range psubscribeCmd.Patterns {
		s.pubsub.Psubscribe(conn, pattern)
		PSubscribersTotal.Increase(1)
		CurrentPSubscribers.Increase(1)
	}
}

func (s *Service) pubsubChannelsCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	pubsubChannelsCmd, err := protocol.ParsePubSubChannelsCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	var channels []string
	if pubsubChannelsCmd.Pattern != "" {
		channels = s.pubsub.ChannelsWithPatterns(pubsubChannelsCmd.Pattern)
	} else {
		channels = s.pubsub.Channels()
	}
	conn.WriteArray(len(channels))
	for _, channel := range channels {
		conn.WriteBulkString(channel)
	}
}

func (s *Service) pubsubNumpatCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	_, err := protocol.ParsePubSubNumpatCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	conn.WriteInt(s.pubsub.Numpat())
}

func (s *Service) pubsubNumsubCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	pubsubNumsubCmd, err := protocol.ParsePubSubNumsubCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	if len(pubsubNumsubCmd.Channels) == 0 {
		conn.WriteArray(0)
		return
	}

	conn.WriteArray(len(pubsubNumsubCmd.Channels) * 2)
	for _, channel := range pubsubNumsubCmd.Channels {
		conn.WriteBulkString(channel)
		conn.WriteInt(s.pubsub.Numsub(channel))
	}
}
