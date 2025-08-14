package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"fmt"
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

//export Connect
func Connect(conninfo *C.char) *C.char {

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

//export Execute
func Execute(handle C.ulonglong, query *C.char, params *C.char, format *C.char) *C.char {

	id := uint64(handle)

	q := C.GoString(query)
	p := C.GoString(params)

	if len(p) == 0 {
		return exec(id, q)
	} else {
		return execParams(id, q, p)
	}

}

//export Query
func Query(handle C.ulonglong, query *C.char, params *C.char, format *C.char) *C.char {

	id := uint64(handle)

	q := C.GoString(query)
	p := C.GoString(params)
	f := C.GoString(format)

	if len(p) == 0 {
		return queryExecute(id, q, f)
	} else {
		return queryParamsExecute(id, q, p, f)
	}

}

//export FreeCString
func FreeCString(p *C.char) { C.free(unsafe.Pointer(p)) }

func main() {}
