package pgx_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnCopyFromContextCancel(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	_, err := conn.Exec(context.Background(), `create temporary table foo (
		id integer
	)`)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	source := &testConnCopyFromContextCancelSource{
		cancel: cancel,
	}

	_, err = conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"id"}, source)
	require.ErrorIs(t, err, context.Canceled)

	// Immediately execute a simple query on the same connection.
	// Since the connection should be closed, this query should fail.
	var val int
	err = conn.QueryRow(context.Background(), "select 1").Scan(&val)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

type testConnCopyFromContextCancelSource struct {
	cancel func()
	called bool
}

func (s *testConnCopyFromContextCancelSource) Next() bool {
	if !s.called {
		s.called = true
		s.cancel()
		return true
	}
	return false
}

func (s *testConnCopyFromContextCancelSource) Values() ([]any, error) {
	return []any{int32(1)}, nil
}

func (s *testConnCopyFromContextCancelSource) Err() error {
	return nil
}