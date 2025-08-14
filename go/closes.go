package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
)

//export Close
func Close(handle C.ulonglong) *C.char {

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
