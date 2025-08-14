package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func jsonErr(err error) []byte {
	msg, _ := json.Marshal(err.Error())
	return []byte(fmt.Sprintf(`{"error":%s}`, string(msg)))
}

// Parse an RFC3339 string as time if look like time.Time
func tryParseTime(s string) (time.Time, bool) {

	var t time.Time
	var err error

	t, err = time.Parse(time.RFC3339Nano, s)

	if err == nil {
		return t, true
	}

	t, err = time.Parse("2006-01-02", s)
	if err == nil {
		return t, true
	}

	return time.Time{}, false

}

// catch args
func jsonToArgs(raw []byte) ([]any, error) {

	if len(raw) == 0 {
		return nil, nil
	}

	var arr []any

	err := json.Unmarshal(raw, &arr)
	if err != nil {
		return nil, err
	}

	out := make([]any, 0, len(arr))

	for _, v := range arr {

		switch value := v.(type) {
		case nil:
			out = append(out, nil)
		case bool:
			out = append(out, value)
		case float64:
			// se for inteiro exato, converte p/ int64
			if value == float64(int64(value)) {
				out = append(out, int64(value))
			} else {
				out = append(out, value)
			}
		case string:
			// é datetime?
			t, ok := tryParseTime(value)
			if ok {
				out = append(out, t)
			} else {
				out = append(out, value)
			}
		default:
			// onj/arrays: JSON string
			b, _ := json.Marshal(value)
			out = append(out, b)
		}

	}

	return out, nil

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
