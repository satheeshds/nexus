package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/jackc/pgproto3/v2"
	"github.com/satheeshds/nexus/internal/pool"
)

// handler runs the query loop for a single client connection.
type handler struct {
	backend *pgproto3.Backend
	session *pool.Session
}

func (h *handler) run(ctx context.Context) {
	for {
		msg, err := h.backend.Receive()
		if err != nil {
			slog.Debug("gateway: connection closed", "tenant", h.session.TenantID)
			return
		}

		switch m := msg.(type) {
		case *pgproto3.Query:
			h.handleQuery(ctx, m.String)

		case *pgproto3.Terminate:
			slog.Info("gateway: client terminated", "tenant", h.session.TenantID)
			return

		default:
			_ = h.backend.Send(&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "0A000",
				Message:  "unsupported message type",
			})
			_ = h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		}
	}
}

func (h *handler) handleQuery(ctx context.Context, query string) {
	slog.Debug("gateway: query", "tenant", h.session.TenantID, "sql", query)

	rows, err := h.session.Conn.QueryContext(ctx, query)
	if err != nil {
		_ = h.backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "42601",
			Message:  err.Error(),
		})
		_ = h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		_ = sendError(h.backend, fmt.Sprintf("column types: %v", err))
		return
	}

	// ── RowDescription ────────────────────────────────────────────────────────
	fields := make([]pgproto3.FieldDescription, len(cols))
	for i, c := range cols {
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(c.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          25, // TEXT OID — simplest safe default
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0, // text format
		}
	}
	_ = h.backend.Send(&pgproto3.RowDescription{Fields: fields})

	// ── DataRow (one per row) ─────────────────────────────────────────────────
	vals := make([]any, len(cols))
	scanPtrs := make([]any, len(cols))
	for i := range vals {
		scanPtrs[i] = &vals[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			slog.Warn("gateway: row scan error", "err", err)
			continue
		}
		dataRow := pgproto3.DataRow{Values: make([][]byte, len(cols))}
		for i, v := range vals {
			dataRow.Values[i] = toBytes(v)
		}
		_ = h.backend.Send(&dataRow)
		rowCount++
	}

	// ── CommandComplete ───────────────────────────────────────────────────────
	_ = h.backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte(fmt.Sprintf("SELECT %d", rowCount)),
	})
	_ = h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

// toBytes converts a Go value to its Postgres text-format wire representation.
func toBytes(v any) []byte {
	if v == nil {
		return nil // NULL
	}
	switch t := v.(type) {
	case []byte:
		return t
	case string:
		return []byte(t)
	case sql.RawBytes:
		return []byte(t)
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}
