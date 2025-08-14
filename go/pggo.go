package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type connWrap struct {
	conn *pgx.Conn
}

type poolWrap struct {
	conn *pgxpool.Pool
}

var (
	hCounter  uint64
	connTable sync.Map // id(uint64) -> *connWrap
)

func jsonErr(err error) []byte {
	msg, _ := json.Marshal(err.Error())
	return []byte(fmt.Sprintf(`{"error":%s}`, string(msg)))
}

func rowsToList(rows pgx.Rows) ([]byte, error) {

	var out [][]any

	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		out = append(out, vals)
	}

	return json.Marshal(out)
}

func rowsToJSON(rows pgx.Rows) ([]byte, error) {

	var out []map[string]any

	field_description := rows.FieldDescriptions()

	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}

		row := make(map[string]any, len(vals))
		for i, fd := range field_description {
			row[string(fd.Name)] = vals[i]
		}
		out = append(out, row)

	}

	return json.Marshal(out)

}

//export ConnectJSON
func ConnectJSON(conninfo *C.char) *C.char {

	ci := C.GoString(conninfo)
	ctx := context.Background()

	c, err := pgx.Connect(ctx, ci)
	if err != nil {
		return C.CString(string(jsonErr(err)))
	}

	id := atomic.AddUint64(&hCounter, 1)
	connTable.Store(id, &connWrap{conn: c})

	resp := fmt.Sprintf(`{"handle":%d}`, id)

	return C.CString(resp)

}

//export ConnectPool
func ConnectPool(conninfo *C.char) *C.char {

	ci := C.GoString(conninfo)
	ctx := context.Background()

	cfg, err := pgxpool.ParseConfig(ci)
	if err != nil {
		return C.CString(string(jsonErr(err)))
	}

	cfg.MaxConns = 10
	cfg.MinConns = 1
	cfg.HealthCheckPeriod = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return C.CString(string(jsonErr(err)))
	}

	id := atomic.AddUint64(&hCounter, 1)
	connTable.Store(id, &poolWrap{conn: pool})

	resp := fmt.Sprintf(`{"handle":%d}`, id)

	return C.CString(resp)

}

//export CloseJSON
func CloseJSON(handle C.ulonglong) *C.char {

	id := uint64(handle)
	v, ok := connTable.Load(id)
	if !ok {
		return C.CString(`{"ok":false, "error":}`)
	}

	w := v.(*connWrap)
	ctx := context.Background()

	err := w.conn.Close(ctx)
	if err != nil {
		return C.CString(string(jsonErr(err)))
	}

	connTable.Delete(id)

	return C.CString(`{"ok":true}`)

}

//export ClosePool
func ClosePool(handle C.ulonglong) *C.char {

	id := uint64(handle)
	v, ok := connTable.Load(id)
	if !ok {
		return C.CString(`{"ok":false, "error":}`)
	}

	w := v.(*poolWrap)

	w.conn.Close()

	connTable.Delete(id)

	return C.CString(`{"ok":true}`)

}

func queryJSON(handle uint64, query string, format string) *C.char {

	var data []byte

	v, ok := connTable.Load(handle)
	if !ok {
		return C.CString(`{"error":"invalid handle"}`)
	}

	ctx := context.Background()

	rows, err := v.(*poolWrap).conn.Query(ctx, query)
	if err != nil {
		return C.CString(string(jsonErr(err)))
	}
	defer rows.Close()

	if strings.ToLower(format) == "json" {
		data, err = rowsToJSON(rows)
		if err != nil {
			return C.CString(string(jsonErr(err)))
		}
	} else {
		data, err = rowsToList(rows)
		if err != nil {
			return C.CString(string(jsonErr(err)))
		}
	}

	return C.CString(string(data))

}

func execJSON(handle uint64, query string) *C.char {

	v, ok := connTable.Load(handle)
	if !ok {
		return C.CString(`{"error":"invalid handle"}`)
	}

	ctx := context.Background()

	ct, err := v.(*poolWrap).conn.Exec(ctx, query)
	if err != nil {
		return C.CString(string(jsonErr(err)))
	}

	resp := fmt.Sprintf(`{"rows_affected":%d}`, ct.RowsAffected())

	return C.CString(resp)

}

func queryParamsJSON(handle uint64, query string, params string, format string) *C.char {

	v, ok := connTable.Load(handle)
	if !ok {
		return C.CString(`{"error":"invalid handle"}`)
	}

	args, err := jsonToArgs([]byte(params))
	if err != nil {
		return C.CString(string(jsonErr(fmt.Errorf("bad params json: %w", err))))
	}

	var data []byte
	ctx := context.Background()
	rows, err := v.(*poolWrap).conn.Query(ctx, query, args...)
	if err != nil {
		return C.CString(string(jsonErr(err)))
	}
	defer rows.Close()

	if strings.ToLower(format) == "json" {
		data, err = rowsToJSON(rows)
		if err != nil {
			return C.CString(string(jsonErr(err)))
		}
	} else {
		data, err = rowsToList(rows)
		if err != nil {
			return C.CString(string(jsonErr(err)))
		}
	}

	return C.CString(string(data))

}

func execParamsJSON(handle uint64, query string, params string) *C.char {

	v, ok := connTable.Load(handle)
	if !ok {
		return C.CString(`{"error":"invalid handle"}`)
	}

	args, err := jsonToArgs([]byte(params))
	if err != nil {
		return C.CString(string(jsonErr(fmt.Errorf("bad params json: %w", err))))
	}

	ctx := context.Background()

	ct, err := v.(*poolWrap).conn.Exec(ctx, query, args...)
	if err != nil {
		return C.CString(string(jsonErr(err)))
	}

	resp := fmt.Sprintf(`{"rows_affected":%d}`, ct.RowsAffected())

	return C.CString(resp)

}

//export Execute
func Execute(handle C.ulonglong, query *C.char, params *C.char, format *C.char) *C.char {

	id := uint64(handle)

	q := C.GoString(query)
	p := C.GoString(params)

	if len(p) == 0 {
		return execJSON(id, q)
	} else {
		return execParamsJSON(id, q, p)
	}

}

//export Query
func Query(handle C.ulonglong, query *C.char, params *C.char, format *C.char) *C.char {

	id := uint64(handle)

	q := C.GoString(query)
	p := C.GoString(params)
	f := C.GoString(format)

	if len(p) == 0 {
		return queryJSON(id, q, f)
	} else {
		return queryParamsJSON(id, q, p, f)
	}

}

//export FreeCString
func FreeCString(p *C.char) { C.free(unsafe.Pointer(p)) }

func main() {}
