package gateway

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgproto3/v2"
	"github.com/satheeshds/nexus/internal/pool"
)

type statement struct {
	query      string
	paramCount int
	paramOIDs  []uint32
}

type portal struct {
	query             string
	params            []any
	resultFormatCodes []int16
}

// handler runs the query loop for a single client connection.
type handler struct {
	backend        *pgproto3.Backend
	session        *pool.Session
	statements     map[string]statement         // statement name -> SQL and param count
	portals        map[string]portal            // portal name -> SQL and params
	tableAutoCache map[string]*tableAutoColumns // table name -> auto-injectable columns (seqid/timestamp cache)
}

func (h *handler) run(ctx context.Context) {
	h.statements = make(map[string]statement)
	h.portals = make(map[string]portal)
	h.tableAutoCache = make(map[string]*tableAutoColumns)

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
				query:             s.query,
				params:            params,
				resultFormatCodes: m.ResultFormatCodes,
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
			h.handleExecute(ctx, p.query, p.params, p.resultFormatCodes)

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

	// For INSERT … RETURNING the describe query SELECT * FROM (INSERT …) is not
	// valid in DuckDB.  Synthesise the RowDescription directly from the
	// RETURNING column list instead.
	if _, returningCols := stripReturningClause(query); returningCols != nil {
		fields := make([]pgproto3.FieldDescription, len(returningCols))
		for i, col := range returningCols {
			fields[i] = pgproto3.FieldDescription{
				Name:         []byte(col),
				DataTypeOID:  returningColOID(col),
				DataTypeSize: -1,
				TypeModifier: -1,
				Format:       0,
			}
		}
		_ = h.backend.Send(&pgproto3.RowDescription{Fields: fields})
		return
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
			DataTypeOID:          duckTypeToOID(c.DatabaseTypeName()),
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0, // text format
		}
	}
	_ = h.backend.Send(&pgproto3.RowDescription{Fields: fields})
}

func (h *handler) handleQuery(ctx context.Context, query string) {
	h.executeSQL(ctx, query, nil, nil, true, true)
}

func (h *handler) handleExecute(ctx context.Context, query string, params []any, resultFormatCodes []int16) {
	h.executeSQL(ctx, query, params, resultFormatCodes, false, false)
}

func (h *handler) executeSQL(ctx context.Context, query string, args []any, resultFormatCodes []int16, sendRowDesc bool, sendReady bool) {
	// Rewrite INSERT statements to inject sequential 'id', 'created_at', and
	// 'updated_at' defaults when those columns exist in the target table but
	// are not present in the incoming INSERT.
	query, args = rewriteInsertDefaults(ctx, h.session.Conn, query, args, h.tableAutoCache)

	slog.Debug("gateway: execute after rewrite", "tenant", h.session.TenantID, "sql", query, "params", len(args))

	// DuckLake does not support RETURNING on INSERT.  When the (possibly
	// rewritten) query contains a RETURNING clause, emulate it in the gateway:
	// strip the clause, execute the plain INSERT, and synthesise the result rows.
	if baseQuery, returningCols := stripReturningClause(query); returningCols != nil {
		h.executeInsertReturning(ctx, query, baseQuery, args, returningCols, resultFormatCodes, sendRowDesc, sendReady)
		return
	}

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
				DataTypeOID:  duckTypeToOID(c.DatabaseTypeName()),
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
			format := int16(0)
			if i < len(resultFormatCodes) {
				format = resultFormatCodes[i]
			} else if len(resultFormatCodes) == 1 {
				format = resultFormatCodes[0]
			}

			if format == 1 {
				dataRow.Values[i] = toBinary(v)
			} else {
				dataRow.Values[i] = toBytes(v)
			}
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

// returningColOID returns the most appropriate Postgres OID for the well-known
// auto-injectable columns that appear in RETURNING clauses.  All other columns
// fall back to TEXT (OID 25) which is what the rest of the gateway uses.
func returningColOID(col string) uint32 {
	switch col {
	case "id":
		return 20 // INT8 / BIGINT
	case "created_at", "updated_at":
		return 1114 // TIMESTAMP
	default:
		return 25 // TEXT
	}
}

// executeInsertReturning handles INSERT … RETURNING by:
//  1. Acquiring the per-session insert mutex to prevent concurrent ID races.
//  2. Pre-computing the sequential id value(s) so they can be returned.
//  3. Replacing the injected scalar subquery(ies) with the literal id(s).
//  4. Executing the plain INSERT (without RETURNING).
//  5. Emitting synthetic DataRow(s) for the RETURNING columns.
//
// Supported RETURNING columns:
//   - id → the pre-computed sequential id (only when the rewriter auto-injected it)
//   - any other column → NULL (safe default; most ORMs only need id)
func (h *handler) executeInsertReturning(
	ctx context.Context,
	fullQuery, baseQuery string,
	args []any,
	returningCols []string,
	resultFormatCodes []int16,
	sendRowDesc bool,
	sendReady bool,
) {
	// Parse the base INSERT to learn the table name and row count.
	m := insertRE.FindStringSubmatch(baseQuery)
	if m == nil {
		// Unrecognised form — pass the original query through so the real
		// DuckLake error surfaces to the client.
		slog.Warn("seqid: RETURNING: unrecognised INSERT form, passing through",
			"tenant", h.session.TenantID, "sql", fullQuery)
		h.executeSQL(ctx, fullQuery, args, resultFormatCodes, sendRowDesc, sendReady)
		return
	}

	tableName := strings.TrimSpace(m[1])
	valuesRaw := strings.TrimSpace(m[3])
	numRows := len(splitValueRows(valuesRaw))
	if numRows == 0 {
		numRows = 1
	}

	// Determine whether we need to synthesise id values.
	needsIDReturn := false
	for _, col := range returningCols {
		if col == "id" {
			needsIDReturn = true
			break
		}
	}

	// Only synthesise id when the rewriter actually auto-injected the scalar
	// subquery.  If the client supplied id explicitly the subquery is absent
	// and we cannot emulate RETURNING id safely.
	if needsIDReturn && !idSubqueryRE(tableName).MatchString(baseQuery) {
		slog.Warn("seqid: RETURNING id requested but id was not auto-injected",
			"tenant", h.session.TenantID, "table", tableName)
		_ = h.backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "0A000",
			Message:  "RETURNING id is only supported when id is auto-generated (omit id from the INSERT column list)",
		})
		if sendReady {
			_ = h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		}
		return
	}

	// Acquire the per-session mutex to serialise the MAX(id) query + INSERT.
	// This prevents two concurrent clients for the same tenant from computing
	// the same base id and inserting duplicate values.
	h.session.InsertMu.Lock()
	defer h.session.InsertMu.Unlock()

	// Pre-compute sequential ids (one SELECT round-trip).
	var ids []int64
	if needsIDReturn {
		ids = precomputeInsertIDs(ctx, h.session.Conn, tableName, numRows)
		if ids == nil {
			// Cannot pre-compute ids — the INSERT would succeed but the client
			// would receive NULL for the returned id, which breaks ORMs.
			// Surface the failure explicitly instead.
			slog.Error("seqid: RETURNING: failed to pre-compute id values",
				"tenant", h.session.TenantID, "table", tableName)
			_ = h.backend.Send(&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "XX000",
				Message:  "could not compute next sequential id for RETURNING clause",
			})
			if sendReady {
				_ = h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			}
			return
		}
	}

	// Replace scalar subqueries in baseQuery with literal id values.
	execQuery := baseQuery
	if len(ids) > 0 {
		execQuery = replaceIDSubqueries(execQuery, tableName, ids)
	}

	// Adjust param slice length.
	actualParams := guessParamCount(execQuery)
	execArgs := args
	if len(execArgs) > actualParams {
		execArgs = execArgs[:actualParams]
	} else {
		for len(execArgs) < actualParams {
			execArgs = append(execArgs, nil)
		}
	}

	slog.Debug("gateway: execute insert (RETURNING emulated)",
		"tenant", h.session.TenantID, "sql", execQuery, "params", len(execArgs),
		"returning", returningCols)

	// Execute the INSERT without RETURNING.
	dbRows, err := h.session.Conn.QueryContext(ctx, execQuery, execArgs...)
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
	dbRows.Close()

	// Send RowDescription (only for simple-query protocol; extended uses Describe).
	if sendRowDesc {
		fields := make([]pgproto3.FieldDescription, len(returningCols))
		for i, col := range returningCols {
			fields[i] = pgproto3.FieldDescription{
				Name:         []byte(col),
				DataTypeOID:  returningColOID(col),
				DataTypeSize: -1,
				TypeModifier: -1,
				Format:       0,
			}
		}
		_ = h.backend.Send(&pgproto3.RowDescription{Fields: fields})
	}

	// Emit one DataRow per inserted row.
	// Only 'id' is supported in RETURNING; all other columns return NULL.
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		dataRow := pgproto3.DataRow{Values: make([][]byte, len(returningCols))}
		for colIdx, col := range returningCols {
			format := int16(0)
			if colIdx < len(resultFormatCodes) {
				format = resultFormatCodes[colIdx]
			} else if len(resultFormatCodes) == 1 {
				format = resultFormatCodes[0]
			}

			if col == "id" && rowIdx < len(ids) {
				if format == 1 {
					dataRow.Values[colIdx] = toBinary(ids[rowIdx])
				} else {
					dataRow.Values[colIdx] = []byte(fmt.Sprintf("%d", ids[rowIdx]))
				}
			}
			// all other columns → nil (NULL)
		}
		_ = h.backend.Send(&dataRow)
	}

	_ = h.backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte(fmt.Sprintf("INSERT 0 %d", numRows)),
	})
	if sendReady {
		_ = h.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	}
}

// duckTypeToOID maps DuckDB/Go driver type names to PostgreSQL OIDs so that
// client drivers can decode column values into the correct native types
// (e.g. time.Time for timestamps, decimal for NUMERIC) instead of receiving
// everything as a plain text string.
func duckTypeToOID(dbTypeName string) uint32 {
	switch strings.ToUpper(dbTypeName) {
	case "BIGINT", "INT8", "HUGEINT", "UBIGINT":
		return 20 // INT8
	case "INTEGER", "INT4", "INT", "SIGNED":
		return 23 // INT4
	case "SMALLINT", "INT2", "SHORT":
		return 21 // INT2
	case "BOOLEAN", "BOOL":
		return 16 // BOOL
	case "REAL", "FLOAT4":
		return 700 // FLOAT4
	case "DOUBLE", "FLOAT8", "FLOAT":
		return 701 // FLOAT8
	case "DECIMAL", "NUMERIC":
		return 1700 // NUMERIC
	case "TIMESTAMP", "DATETIME":
		return 1114 // TIMESTAMP
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return 1184 // TIMESTAMPTZ
	case "DATE":
		return 1082 // DATE
	case "TIME":
		return 1083 // TIME
	case "INTERVAL":
		return 1186 // INTERVAL
	case "UUID":
		return 2950 // UUID
	case "BLOB", "BYTEA":
		return 17 // BYTEA
	default:
		return 25 // TEXT
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

func toBinary(v any) []byte {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case int64:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(t))
		return b
	case int32:
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(t))
		return b
	case int:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(t))
		return b
	case []byte:
		return t
	default:
		return toBytes(v)
	}
}
