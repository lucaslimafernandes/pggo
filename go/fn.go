package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"fmt"
	"strings"
)

func queryExecute(handle uint64, query string, format string) *C.char {

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

func exec(handle uint64, query string) *C.char {

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

func queryParamsExecute(handle uint64, query string, params string, format string) *C.char {

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

func execParams(handle uint64, query string, params string) *C.char {

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
