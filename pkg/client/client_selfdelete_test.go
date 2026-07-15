// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package client_test

import (
	"context"
	"crypto/aes"
	"net"
	"sync"
	"testing"
	"time"

	clientpb "github.com/siderolabs/discovery-api/api/v1alpha1/client/pb"
	serverpb "github.com/siderolabs/discovery-api/api/v1alpha1/server/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/siderolabs/discovery-client/pkg/client"
)

// fakeClusterServer is a minimal in-memory discovery service used to drive the client
// through scenarios which are hard to reproduce against a real server (e.g. server-side
// loss of the client's own affiliate).
type fakeClusterServer struct {
	serverpb.UnimplementedClusterServer

	updates chan *serverpb.AffiliateUpdateRequest
	deletes chan *serverpb.AffiliateDeleteRequest

	watchersMu sync.Mutex
	watchers   []chan *serverpb.WatchResponse
}

func (srv *fakeClusterServer) Hello(context.Context, *serverpb.HelloRequest) (*serverpb.HelloResponse, error) {
	return &serverpb.HelloResponse{}, nil
}

func (srv *fakeClusterServer) AffiliateUpdate(_ context.Context, req *serverpb.AffiliateUpdateRequest) (*serverpb.AffiliateUpdateResponse, error) {
	srv.updates <- req

	return &serverpb.AffiliateUpdateResponse{}, nil
}

func (srv *fakeClusterServer) AffiliateDelete(_ context.Context, req *serverpb.AffiliateDeleteRequest) (*serverpb.AffiliateDeleteResponse, error) {
	srv.deletes <- req

	return &serverpb.AffiliateDeleteResponse{}, nil
}

func (srv *fakeClusterServer) Watch(_ *serverpb.WatchRequest, stream grpc.ServerStreamingServer[serverpb.WatchResponse]) error {
	// initial snapshot
	if err := stream.Send(&serverpb.WatchResponse{}); err != nil {
		return err
	}

	ch := make(chan *serverpb.WatchResponse, 8)

	srv.watchersMu.Lock()
	srv.watchers = append(srv.watchers, ch)
	srv.watchersMu.Unlock()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case resp := <-ch:
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

func (srv *fakeClusterServer) pushToWatchers(t *testing.T, resp *serverpb.WatchResponse) {
	t.Helper()

	// wait for at least one watcher to be registered
	deadline := time.Now().Add(5 * time.Second)

	for {
		srv.watchersMu.Lock()
		watchers := append([]chan *serverpb.WatchResponse(nil), srv.watchers...)
		srv.watchersMu.Unlock()

		if len(watchers) > 0 {
			for _, ch := range watchers {
				ch <- resp
			}

			return
		}

		require.False(t, time.Now().After(deadline), "no watchers registered")

		time.Sleep(10 * time.Millisecond)
	}
}

// waitForInitialAnnounce receives the first AffiliateUpdate, then drains any follow-up
// updates until the client goes quiet (SetLocalData before Run leaves a pending local
// update notification, so the client may legitimately announce more than once at startup).
func (srv *fakeClusterServer) waitForInitialAnnounce(t *testing.T, affiliateID string) {
	t.Helper()

	select {
	case req := <-srv.updates:
		assert.Equal(t, affiliateID, req.AffiliateId)
	case <-time.After(10 * time.Second):
		require.Fail(t, "no initial affiliate update")
	}

	for {
		select {
		case req := <-srv.updates:
			assert.Equal(t, affiliateID, req.AffiliateId)
		case <-time.After(2 * time.Second):
			return
		}
	}
}

func setupFakeServer(t *testing.T, ctx context.Context, affiliateID string) (*fakeClusterServer, *client.Client, *observer.ObservedLogs) {
	t.Helper()

	srv := &fakeClusterServer{
		updates: make(chan *serverpb.AffiliateUpdateRequest, 16),
		deletes: make(chan *serverpb.AffiliateDeleteRequest, 16),
	}

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	serverpb.RegisterClusterServer(grpcServer, srv)

	go grpcServer.Serve(lis) //nolint:errcheck

	t.Cleanup(grpcServer.Stop)

	cipher, err := aes.NewCipher(make([]byte, 32))
	require.NoError(t, err)

	c, err := client.NewClient(client.Options{
		Cipher:      cipher,
		Endpoint:    "passthrough:///bufnet",
		ClusterID:   randomID(t),
		AffiliateID: affiliateID,
		// long TTL, so that the refresh ticker doesn't fire during the test
		TTL:      time.Hour,
		Insecure: true,
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
		},
	})
	require.NoError(t, err)

	require.NoError(t, c.SetLocalData(&client.Affiliate{
		Affiliate: &clientpb.Affiliate{
			NodeId:   affiliateID,
			Hostname: "host1",
		},
	}, nil))

	observedCore, observedLogs := observer.New(zap.DebugLevel)
	logger := zap.New(zapcore.NewTee(zaptest.NewLogger(t).Core(), observedCore))

	notify := make(chan struct{}, 1)
	done := make(chan error, 1)

	runCtx, runCancel := context.WithCancel(ctx)

	go func() {
		done <- c.Run(runCtx, logger, notify)
	}()

	t.Cleanup(func() {
		runCancel()
		assert.NoError(t, <-done)
	})

	return srv, c, observedLogs
}

func TestReannounceOnSelfDeletion(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	affiliateID := randomID(t)

	srv, _, observedLogs := setupFakeServer(t, ctx, affiliateID)

	srv.waitForInitialAnnounce(t, affiliateID)

	// simulate server-side loss of the affiliate: TTL GC pushes a deletion event to watchers
	srv.pushToWatchers(t, &serverpb.WatchResponse{
		Deleted: true,
		Affiliates: []*serverpb.Affiliate{
			{
				Id: affiliateID,
			},
		},
	})

	// client should detect its own deletion and re-announce
	select {
	case req := <-srv.updates:
		assert.Equal(t, affiliateID, req.AffiliateId)
	case <-time.After(10 * time.Second):
		require.Fail(t, "client did not re-announce after server-side deletion of its affiliate")
	}

	assert.Equal(t, 1, observedLogs.FilterMessage("local affiliate was deleted server-side, re-announcing").Len())
}

func TestNoReannounceAfterLocalDelete(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	affiliateID := randomID(t)

	srv, c, observedLogs := setupFakeServer(t, ctx, affiliateID)

	srv.waitForInitialAnnounce(t, affiliateID)

	c.DeleteLocalAffiliate()

	select {
	case req := <-srv.deletes:
		assert.Equal(t, affiliateID, req.AffiliateId)
	case <-time.After(10 * time.Second):
		require.Fail(t, "no affiliate delete")
	}

	// the deletion event for our own affiliate is now expected, and should not trigger a re-announce
	srv.pushToWatchers(t, &serverpb.WatchResponse{
		Deleted: true,
		Affiliates: []*serverpb.Affiliate{
			{
				Id: affiliateID,
			},
		},
	})

	select {
	case <-srv.updates:
		require.Fail(t, "unexpected affiliate update after local delete")
	case <-time.After(2 * time.Second):
	}

	assert.Equal(t, 0, observedLogs.FilterMessage("local affiliate was deleted server-side, re-announcing").Len())
}
