package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgproto3/v2"
	"github.com/satheeshds/nexus/internal/pool"
)

type portal struct {
	query      string
	parameters []any
}

type preparedStatement struct {
	query     string
	paramOIDs []uint32
}

// handler runs the query loop for a single client connection.
type handler struct {
	backend            *pgproto3.Backend
	session            *pool.Session
	preparedStatements map[string]preparedStatement
	portals            map[string]portal
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
			slog.Debug("gateway: msg Query", "sql", m.String)
			h.handleQuery(ctx, m.String, true)

		case *pgproto3.Parse:
			slog.Debug("gateway: msg Parse", "name", m.Name, "sql", m.Query)
			if h.preparedStatements == nil {
				h.preparedStatements = make(map[string]preparedStatement)
			}
			h.preparedStatements[m.Name] = preparedStatement{
				query:     m.Query,
				paramOIDs: m.ParameterOIDs,
			}
			_ = h.backend.Send(&pgproto3.ParseComplete{})

		case *pgproto3.Bind:
			slog.Debug("gateway: msg Bind", "portal", m.DestinationPortal, "stmt", m.PreparedStatement)
			if h.portals == nil {
				h.portals = make(map[string]portal)
			}
			ps := h.preparedStatements[m.PreparedStatement]
			p := portal{query: ps.query}
			for _, b := range m.Parameters {
				p.parameters = append(p.parameters, string(b))
			}
			h.portals[m.DestinationPortal] = p
			_ = h.backend.Send(&pgproto3.BindComplete{})

		case *pgproto3.Describe:
			slog.Debug("gateway: msg Describe", "type", m.ObjectType, "name", m.Name)
			h.handleDescribe(ctx, m)

		case *pgproto3.Execute:
			slog.Debug("gateway: msg Execute", "portal", m.Portal)
			p := h.portals[m.Portal]
			h.handleQuery(ctx, p.query, false, p.parameters...)

		case *pgproto3.Close:
			slog.Debug("gateway: msg Close", "type", m.ObjectType, "name", m.Name)
			_ = h.backend.Send(&pgproto3.CloseComplete{})

		case *pgproto3.Sync:
			slog.Debug("gateway: msg Sync")
			_ = h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

		case *pgproto3.Flush:
			slog.Debug("gateway: msg Flush")
			continue

		case *pgproto3.Terminate:
			slog.Info("gateway: client terminated", "tenant", h.session.TenantID)
			return

		default:
			slog.Warn("gateway: unsupported message type", "type", fmt.Sprintf("%T", msg), "tenant", h.session.TenantID)
			_ = h.backend.Send(&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "0A000",
				Message:  fmt.Sprintf("unsupported message type: %T", msg),
			})
			_ = h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		}
	}
}

func (h *handler) handleDescribe(ctx context.Context, m *pgproto3.Describe) {
	var query string
	var args []any

	if m.ObjectType == 'S' {
		// Describing a Prepared Statement
		ps := h.preparedStatements[m.Name]
		query = ps.query
		// For statements, we must return the OIDs we recorded during Parse
		_ = h.backend.Send(&pgproto3.ParameterDescription{
			ParameterOIDs: ps.paramOIDs,
		})
	} else {
		// Describing a Portal
		p := h.portals[m.Name]
		query = p.query
		args = p.parameters
	}

	if query == "" {
		_ = h.backend.Send(&pgproto3.NoData{})
		return
	}

	// For compatibility interceptions, we must apply the same logic as handleQuery
	trimmed := strings.TrimSpace(strings.ToLower(query))
	if strings.Contains(trimmed, "pg_catalog.pg_database") {
		query = strings.ReplaceAll(query, "datallowconn", "true")
		query = strings.ReplaceAll(query, "datistemplate", "false")
	}

	// Clean query for subquery wrapping: remove trailing semicolon
	cleanQuery := strings.TrimRight(strings.TrimSpace(query), ";")

	// We use QueryContext with a 0-row limit to get metadata quickly.
	// Note: We use a subquery to avoid syntax errors with ORDER BY or other clauses.
	metaQuery := fmt.Sprintf("SELECT * FROM (%s) WHERE 1=0", cleanQuery)
	rows, err := h.session.Conn.QueryContext(ctx, metaQuery, args...)
	if err != nil {
		// If metadata query fails, it might not be a row-returning command (like SET)
		_ = h.backend.Send(&pgproto3.NoData{})
		return
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil || len(cols) == 0 {
		_ = h.backend.Send(&pgproto3.NoData{})
		return
	}

	h.sendRowDescription(cols)
}

func (h *handler) sendRowDescription(cols []*sql.ColumnType) {
	fields := make([]pgproto3.FieldDescription, len(cols))
	for i, c := range cols {
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(c.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          25, // TEXT OID
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0, // text format
		}
	}
	_ = h.backend.Send(&pgproto3.RowDescription{Fields: fields})
}

func (h *handler) handleQuery(ctx context.Context, query string, sendReady bool, args ...any) {
	slog.Debug("gateway: query", "tenant", h.session.TenantID, "sql", query)

	// Intercept certain Postgres-specific session setup queries that DuckDB doesn't support
	// but are harmless to ignore for most clients.
	trimmed := strings.TrimSpace(strings.ToLower(query))

	// ── Compatibility Interceptions ──────────────────────────────────────────
	if strings.Contains(trimmed, "pg_catalog.pg_database") {
		// Mock out common metadata columns that DuckDB is missing but DBeaver expects.
		query = strings.ReplaceAll(query, "datallowconn", "true")
		query = strings.ReplaceAll(query, "datistemplate", "false")
	}

	if strings.HasPrefix(trimmed, "set extra_float_digits") ||
		strings.HasPrefix(trimmed, "set application_name") ||
		strings.HasPrefix(trimmed, "set client_encoding") ||
		strings.HasPrefix(trimmed, "set client_min_messages") ||
		strings.HasPrefix(trimmed, "set bytea_output") {
		_ = h.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SET")})
		if sendReady {
			_ = h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		}
		return
	}

	rows, err := h.session.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		_ = h.backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "42601",
			Message:  err.Error(),
		})
		if sendReady {
			_ = h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		}
		return
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		_ = sendError(h.backend, fmt.Sprintf("column types: %v", err))
		return
	}

	if len(cols) > 0 {
		h.sendRowDescription(cols)
	}

	// ── DataRow (one per row) ─────────────────────────────────────────────────
	vals := make([]any, len(cols))
	scanPtrs := make([]any, len(cols))
	for i := range vals {
		scanPtrs[i] = &vals[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			slog.Warn("gateway: row scan error", "tenant", h.session.TenantID, "row", rowCount, "err", err)
			if sendErr := h.backend.Send(&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "XX000",
				Message:  fmt.Sprintf("row scan error at row %d: %v", rowCount, err),
			}); sendErr != nil {
				slog.Warn("gateway: failed to send scan error response", "err", sendErr)
			}
			if sendReady {
				if sendErr := h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'}); sendErr != nil {
					slog.Warn("gateway: failed to send ReadyForQuery after scan error", "err", sendErr)
				}
			}
			return
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
	if sendReady {
		_ = h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	}
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
