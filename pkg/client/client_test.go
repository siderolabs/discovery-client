// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package client_test

import (
	"context"
	"crypto/aes"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"testing"
	"time"

	clientpb "github.com/siderolabs/discovery-api/api/v1alpha1/client/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap/zaptest"
	"golang.org/x/sync/errgroup"

	"github.com/siderolabs/discovery-client/pkg/client"
)

func randomID(t *testing.T) string {
	t.Helper()

	id := make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, id)
	require.NoError(t, err)

	return hex.EncodeToString(id)
}

//nolint:cyclop,gocyclo
func TestSmokeTest(t *testing.T) {
	t.Parallel()

	// This is a smoke test for the client package using the public discovery.talos.dev instance.
	// The full test suite is in the siderolabs/discovery-service repository.
	const endpoint = "discovery.talos.dev:443"

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	logger := zaptest.NewLogger(t)

	clusterID := randomID(t)

	key := make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, key)
	require.NoError(t, err)

	cipher, err := aes.NewCipher(key)
	require.NoError(t, err)

	affiliate1 := randomID(t)
	affiliate2 := randomID(t)

	client1, err := client.NewClient(client.Options{
		Cipher:      cipher,
		Endpoint:    endpoint,
		ClusterID:   clusterID,
		AffiliateID: affiliate1,
		TTL:         time.Minute,
		TLSConfig:   &tls.Config{},
	})
	require.NoError(t, err)

	client2, err := client.NewClient(client.Options{
		Cipher:      cipher,
		Endpoint:    endpoint,
		ClusterID:   clusterID,
		AffiliateID: affiliate2,
		TTL:         time.Minute,
		TLSConfig:   &tls.Config{},
	})
	require.NoError(t, err)

	notify1 := make(chan struct{}, 1)
	notify2 := make(chan struct{}, 1)

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return client1.Run(ctx, logger, notify1)
	})

	eg.Go(func() error {
		return client2.Run(ctx, logger, notify2)
	})

	select {
	case <-notify1:
	case <-time.After(2 * time.Second):
		require.Fail(t, "no initial snapshot update")
	}

	assert.Empty(t, client1.GetAffiliates())

	select {
	case <-notify2:
	case <-time.After(2 * time.Second):
		require.Fail(t, "no initial snapshot update")
	}

	assert.Empty(t, client2.GetAffiliates())

	affiliate1PB := &client.Affiliate{
		Affiliate: &clientpb.Affiliate{
			NodeId:      affiliate1,
			Addresses:   [][]byte{{1, 2, 3}},
			Hostname:    "host1",
			Nodename:    "node1",
			MachineType: "controlplane",
		},
	}

	require.NoError(t, client1.SetLocalData(affiliate1PB, nil))

	affiliate2PB := &client.Affiliate{
		Affiliate: &clientpb.Affiliate{
			NodeId:      affiliate2,
			Addresses:   [][]byte{{2, 3, 4}},
			Hostname:    "host2",
			Nodename:    "node2",
			MachineType: "worker",
		},
	}

	require.NoError(t, client2.SetLocalData(affiliate2PB, nil))

	// both clients should eventually discover each other

	for {
		t.Logf("client1 affiliates = %d", len(client1.GetAffiliates()))

		if len(client1.GetAffiliates()) == 1 {
			break
		}

		select {
		case <-notify1:
		case <-time.After(2 * time.Second):
			t.Logf("client1 affiliates on timeout = %d", len(client1.GetAffiliates()))

			require.Fail(t, "no incremental update")
		}
	}

	require.Len(t, client1.GetAffiliates(), 1)

	assert.Equal(t, []*client.Affiliate{affiliate2PB}, client1.GetAffiliates())

	for {
		t.Logf("client2 affiliates = %d", len(client1.GetAffiliates()))

		if len(client2.GetAffiliates()) == 1 {
			break
		}

		select {
		case <-notify2:
		case <-time.After(2 * time.Second):
			require.Fail(t, "no incremental update")
		}
	}

	require.Len(t, client2.GetAffiliates(), 1)

	assert.Equal(t, []*client.Affiliate{affiliate1PB}, client2.GetAffiliates())

	// update affiliate1, client2 should see the update
	affiliate1PB.Endpoints = []*clientpb.Endpoint{
		{
			Ip:   []byte{1, 2, 3, 4},
			Port: 5678,
		},
	}
	require.NoError(t, client1.SetLocalData(affiliate1PB, nil))

	for {
		select {
		case <-notify2:
		case <-time.After(time.Second):
			require.Fail(t, "no incremental update")
		}

		if len(client2.GetAffiliates()[0].Endpoints) == 1 {
			break
		}
	}

	assert.Equal(t, []*client.Affiliate{affiliate1PB}, client2.GetAffiliates())

	// delete affiliate1, client2 should see the update
	client1.DeleteLocalAffiliate()

	for {
		select {
		case <-notify2:
		case <-time.After(time.Second):
			require.Fail(t, "no incremental update")
		}

		if len(client2.GetAffiliates()) == 0 {
			break
		}
	}

	require.Len(t, client2.GetAffiliates(), 0)

	cancel()

	err = eg.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		assert.NoError(t, err)
	}
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
