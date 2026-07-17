package pgx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

// CopyFromRows returns a CopyFromSource interface over the provided rows slice.
func CopyFromRows(rows [][]any) CopyFromSource {
	return &copyFromRows{rows: rows, idx: -1}
}

type copyFromRows struct {
	rows [][]any
	idx  int
}

func (ctr *copyFromRows) Next() bool {
	ctr.idx++
	return ctr.idx < len(ctr.rows)
}

func (ctr *copyFromRows) Values() ([]any, error) {
	return ctr.rows[ctr.idx], nil
}

func (ctr *copyFromRows) Err() error {
	return nil
}

// CopyFromSlice returns a CopyFromSource interface over the provided slice.
// The len of slice elements must be equal to the len of columnNames.
// Deprecated: Use CopyFromRows instead.
func CopyFromSlice(rows [][]any) CopyFromSource {
	return CopyFromRows(rows)
}

// CopyFromSource is the interface used by CopyFrom as the source for copy data.
type CopyFromSource interface {
	// Next returns true if there is another row and makes the next row data
	// available to Values(). Next should return false after the last row or if
	// an error occurs.
	Next() bool

	// Values returns the values for the current row.
	Values() ([]any, error)

	// Err returns any error that has occurred during Next or Values.
	Err() error
}

type copyFromReader struct {
	conn          *Conn
	rowSrc        CopyFromSource
	columnNames   []string
	readerErrChan chan error

	buf []byte
	err error
}

func (r *copyFromReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}

	for len(r.buf) == 0 {
		if !r.rowSrc.Next() {
			r.err = r.rowSrc.Err()
			if r.err == nil {
				r.err = io.EOF
			}
			return 0, r.err
		}

		values, err := r.rowSrc.Values()
		if err != nil {
			r.err = err
			return 0, r.err
		}

		if len(values) != len(r.columnNames) {
			r.err = fmt.Errorf("expected %d values, got %d", len(r.columnNames), len(values))
			return 0, r.err
		}

		if r.buf == nil {
			r.buf = make([]byte, 0, 1024)
			r.buf = append(r.buf, "PGCOPY\n\xff\r\n\x00"...) // signature
			r.buf = append(r.buf, 0, 0, 0, 0)                // flags
			r.buf = append(r.buf, 0, 0, 0, 0)                // header extension length
		}

		r.buf = append(r.buf, 0, 0) // number of fields
		numFields := len(values)
		r.buf[len(r.buf)-2] = byte(numFields >> 8)
		r.buf[len(r.buf)-1] = byte(numFields)

		for i, val := range values {
			if val == nil {
				r.buf = append(r.buf, 0xff, 0xff, 0xff, 0xff)
				continue
			}

			sp := len(r.buf)
			r.buf = append(r.buf, 0, 0, 0, 0) // length placeholder

			var err error
			r.buf, err = r.conn.TypeMap().Encode(r.conn.TypeMap().Format(r.columnNames[i], BinaryFormatCode), r.columnNames[i], val, r.buf)
			if err != nil {
				r.err = err
				return 0, r.err
			}

			size := len(r.buf) - sp - 4
			r.buf[sp] = byte(size >> 24)
			r.buf[sp+1] = byte(size >> 16)
			r.buf[sp+2] = byte(size >> 8)
			r.buf[sp+3] = byte(size)
		}
	}

	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// CopyFrom uses the PostgreSQL binary copy protocol to perform bulk inserts.
// It is the most efficient way to insert a large number of rows into a table.
//
// tableName can be a Identifier or a string. If it is a string, it will be
// parsed as a Identifier.
//
// columnNames is the list of columns to insert into.
//
// rowSrc is the source of the data to insert.
//
// CopyFrom is not query safe. It must be the only query running on the connection.
func (c *Conn) CopyFrom(ctx context.Context, tableName Identifier, columnNames []string, rowSrc CopyFromSource) (int64, error) {
	r := &copyFromReader{
		conn:          c,
		rowSrc:        rowSrc,
		columnNames:   columnNames,
		readerErrChan: make(chan error, 1),
	}

	var quotedColumnNames []string
	for _, cn := range columnNames {
		quotedColumnNames = append(quotedColumnNames, quoteIdentifier(cn))
	}

	sql := fmt.Sprintf("copy %s (%s) from stdin binary", tableName.Sanitize(), strings.Join(quotedColumnNames, ", "))

	commandTag, err := c.pgConn.CopyFrom(ctx, r, sql)
	if err != nil {
		if ctx.Err() != nil {
			c.Close(context.Background())
		}
		return 0, err
	}

	return commandTag.RowsAffected(), nil
}