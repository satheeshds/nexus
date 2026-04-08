package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/jackc/pgproto3/v2"
	"github.com/satheeshds/nexus/internal/pool"
)

type statement struct {
	query      string
	paramCount int
	paramOIDs  []uint32
}

type portal struct {
	query  string
	params []any
}

// handler runs the query loop for a single client connection.
type handler struct {
	backend      *pgproto3.Backend
	session      *pool.Session
	statements   map[string]statement // statement name -> SQL and param count
	portals      map[string]portal    // portal name -> SQL and params
	tableIDCache map[string]bool      // table name -> has integer 'id' column (seqid cache)
}

func (h *handler) run(ctx context.Context) {
	h.statements = make(map[string]statement)
	h.portals = make(map[string]portal)
	h.tableIDCache = make(map[string]bool)

	for {
		msg, err := h.backend.Receive()
		if err != nil {
			slog.Debug("gateway: connection closed", "tenant", h.session.TenantID)
			return
		}

		slog.Debug("gateway: message", "tenant", h.session.TenantID, "msg", fmt.Sprintf("%T", msg))

		switch m := msg.(type) {
		case *pgproto3.Query:
			h.handleQuery(ctx, m.String)

		case *pgproto3.Parse:
			pCount := len(m.ParameterOIDs)
			if pCount == 0 && m.Query != "" {
				pCount = guessParamCount(m.Query)
			}
			h.statements[m.Name] = statement{
				query:      m.Query,
				paramCount: pCount,
				paramOIDs:  m.ParameterOIDs,
			}
			_ = h.backend.Send(&pgproto3.ParseComplete{})

		case *pgproto3.Bind:
			s := h.statements[m.PreparedStatement]
			params := make([]any, len(m.Parameters))
			for i, p := range m.Parameters {
				if p == nil {
					params[i] = nil
				} else {
					params[i] = string(p)
				}
			}
			h.portals[m.DestinationPortal] = portal{
				query:  s.query,
				params: params,
			}
			_ = h.backend.Send(&pgproto3.BindComplete{})

		case *pgproto3.Describe:
			if m.ObjectType == 'S' {
				s := h.statements[m.Name]
				h.handleDescribe(ctx, 'S', s.query, s.paramOIDs)
			} else {
				p := h.portals[m.Name]
				// For portals, we can use the actual parameters if we have them,
				// but for schema description LIMIT 0 with nils is usually fine.
				h.handleDescribe(ctx, 'P', p.query, nil)
			}

		case *pgproto3.Execute:
			p := h.portals[m.Portal]
			h.handleExecute(ctx, p.query, p.params)

		case *pgproto3.Sync:
			_ = h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

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

func (h *handler) handleDescribe(ctx context.Context, objectType byte, query string, paramOIDs []uint32) {
	if query == "" {
		if objectType == 'S' {
			_ = h.backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{}})
		}
		_ = h.backend.Send(&pgproto3.NoData{})
		return
	}

	actualParams := guessParamCount(query)
	slog.Debug("gateway: describe", "tenant", h.session.TenantID, "sql", query, "params", actualParams)

	if objectType == 'S' {
		outOIDs := make([]uint32, actualParams)
		copy(outOIDs, paramOIDs)
		_ = h.backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: outOIDs})
	}

	// 2. Get RowDescription (execute with LIMIT 0 to get schema)
	// Some simple SQL optimization here for DuckDB
	describeQuery := fmt.Sprintf("SELECT * FROM (%s) AS __gateway_describe LIMIT 0", query)

	actualParams = guessParamCount(query)
	args := make([]any, actualParams)
	for i := range args {
		args[i] = nil
	}

	rows, err := h.session.Conn.QueryContext(ctx, describeQuery, args...)
	if err != nil {
		slog.Warn("gateway: describe error", "err", err, "sql", query)
		_ = h.backend.Send(&pgproto3.NoData{})
		return
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		_ = h.backend.Send(&pgproto3.NoData{})
		return
	}

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

func (h *handler) handleQuery(ctx context.Context, query string) {
	h.executeSQL(ctx, query, nil, true, true)
}

func (h *handler) handleExecute(ctx context.Context, query string, params []any) {
	h.executeSQL(ctx, query, params, false, false)
}

func (h *handler) executeSQL(ctx context.Context, query string, args []any, sendRowDesc bool, sendReady bool) {
	// Rewrite INSERT statements to inject a sequential 'id' when the target
	// table has an integer id column and no id value was provided.
	query, args = rewriteInsertForSequentialID(ctx, h.session.Conn, query, args, h.tableIDCache)

	actualParams := guessParamCount(query)
	if len(args) > actualParams {
		args = args[:actualParams]
	} else if len(args) < actualParams {
		for len(args) < actualParams {
			args = append(args, nil)
		}
	}

	slog.Debug("gateway: execute", "tenant", h.session.TenantID, "sql", query, "params", len(args))

	rows, err := h.session.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		slog.Error("gateway: execution error", "tenant", h.session.TenantID, "err", err)
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
		_ = h.backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: err.Error()})
		if sendReady {
			_ = h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		}
		return
	}

	if sendRowDesc {
		fields := make([]pgproto3.FieldDescription, len(cols))
		for i, c := range cols {
			fields[i] = pgproto3.FieldDescription{
				Name:         []byte(c.Name()),
				DataTypeOID:  25, // TEXT
				DataTypeSize: -1,
				TypeModifier: -1,
				Format:       0,
			}
		}
		_ = h.backend.Send(&pgproto3.RowDescription{Fields: fields})
	}

	vals := make([]any, len(cols))
	scanPtrs := make([]any, len(cols))
	for i := range vals {
		scanPtrs[i] = &vals[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			slog.Warn("gateway: scan error", "err", err)
			break
		}
		dataRow := pgproto3.DataRow{Values: make([][]byte, len(cols))}
		for i, v := range vals {
			dataRow.Values[i] = toBytes(v)
		}
		_ = h.backend.Send(&dataRow)
		rowCount++
	}

	_ = h.backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte(fmt.Sprintf("SELECT %d", rowCount)),
	})

	if sendReady {
		_ = h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	}
}

func toBytes(v any) []byte {
	if v == nil {
		return nil
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

func guessParamCount(sql string) int {
	maxParam := 0
	for i := 0; i < len(sql); i++ {
		if sql[i] == '$' {
			j := i + 1
			num := 0
			hasNum := false
			for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
				num = num*10 + int(sql[j]-'0')
				hasNum = true
				j++
			}
			if hasNum && num > maxParam {
				maxParam = num
			}
		}
	}
	return maxParam
}
