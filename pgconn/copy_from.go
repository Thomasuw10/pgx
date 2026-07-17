package pgconn

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgproto3"
)

func (c *Conn) CopyFrom(ctx context.Context, r io.Reader, sql string) (CommandTag, error) {
	if err := c.Lock(); err != nil {
		return CommandTag{}, err
	}
	defer c.Unlock()

	if c.status != connStatusIdle {
		return CommandTag{}, errInProgress
	}

	err := c.writeCopyFrom(ctx, sql)
	if err != nil {
		return CommandTag{}, err
	}

	c.status = connStatusCopyIn

	err = c.readCopyInResponse(ctx)
	if err != nil {
		c.die(err)
		return CommandTag{}, err
	}

	err = c.sendCopyData(ctx, r)
	if err != nil {
		c.die(err)
		return CommandTag{}, err
	}

	commandTag, err := c.readCopyOutResponse(ctx)
	if err != nil {
		if c.status != connStatusIdle {
			c.die(err)
		}
		return CommandTag{}, err
	}

	return commandTag, nil
}

func (c *Conn) writeCopyFrom(ctx context.Context, sql string) error {
	c.wbuf = append(c.wbuf, 'Q')
	sp := len(c.wbuf)
	c.wbuf = append(c.wbuf, 0, 0, 0, 0)
	c.wbuf = append(c.wbuf, sql...)
	c.wbuf = append(c.wbuf, 0)
	c.writeByteLength(sp)

	return c.flush(ctx)
}

func (c *Conn) readCopyInResponse(ctx context.Context) error {
	msg, err := c.rxMsg()
	if err != nil {
		return err
	}

	switch msg := msg.(type) {
	case *pgproto3.CopyInResponse:
		return nil
	case *pgproto3.ErrorResponse:
		c.status = connStatusIdle
		return ErrorResponseToPgError(msg)
	default:
		return fmt.Errorf("unexpected message: %T", msg)
	}
}

func (c *Conn) sendCopyData(ctx context.Context, r io.Reader) error {
	buf := c.wbuf
	for {
		if len(buf) > 0 {
			c.wbuf = buf
			err := c.flush(ctx)
			if err != nil {
				return err
			}
			buf = c.wbuf[:0]
		}

		buf = append(buf, 'd')
		sp := len(buf)
		buf = append(buf, 0, 0, 0, 0)

		if cap(buf)-len(buf) < 4096 {
			temp := make([]byte, len(buf), len(buf)+4096)
			copy(temp, buf)
			buf = temp
		}

		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]

		if err != nil {
			if err == io.EOF {
				c.wbuf = buf[:0]
				c.wbuf = append(c.wbuf, 'c')
				c.wbuf = append(c.wbuf, 0, 0, 0, 4)
				return c.flush(ctx)
			}

			c.wbuf = buf[:0]
			c.wbuf = append(c.wbuf, 'f')
			fsp := len(c.wbuf)
			c.wbuf = append(c.wbuf, 0, 0, 0, 0)
			c.wbuf = append(c.wbuf, err.Error()...)
			c.wbuf = append(c.wbuf, 0)
			c.writeByteLength(fsp)
			flushErr := c.flush(ctx)
			if flushErr != nil {
				return flushErr
			}

			return err
		}

		c.writeByteLength(sp)
	}
}

func (c *Conn) readCopyOutResponse(ctx context.Context) (CommandTag, error) {
	for {
		msg, err := c.rxMsg()
		if err != nil {
			return CommandTag{}, err
		}

		switch msg := msg.(type) {
		case *pgproto3.CommandComplete:
			c.status = connStatusIdle
			return CommandTag(msg.CommandTag), nil
		case *pgproto3.ErrorResponse:
			c.status = connStatusIdle
			return CommandTag{}, ErrorResponseToPgError(msg)
		default:
			return CommandTag{}, fmt.Errorf("unexpected message during copy: %T", msg)
		}
	}
}