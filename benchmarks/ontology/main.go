package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"net/http"
	"os"
	"runtime"
	"strconv"
)

var queries map[string]string
var db *sql.DB

func one(c *sql.Conn, name string, args ...any) (any, error) {
	var raw string
	err := c.QueryRowContext(context.Background(), queries[name], args...).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var value any
	err = json.Unmarshal([]byte(raw), &value)
	return value, err
}

func handle(w http.ResponseWriter, r *http.Request) {
	c, err := db.Conn(context.Background())
	if err != nil {
		w.WriteHeader(500)
		return
	}
	defer c.Close()
	locked, began := false, false
	defer func() {
		if began {
			c.ExecContext(context.Background(), "ROLLBACK")
		}
		if locked {
			c.ExecContext(context.Background(), "SELECT pg_advisory_unlock(91011)")
		}
	}()
	exec := func(s string) error { _, e := c.ExecContext(context.Background(), s); return e }
	if r.URL.Path == "/action" {
		if exec("SELECT pg_advisory_lock(91011)") != nil {
			w.WriteHeader(500)
			return
		}
		locked = true
		if exec("BEGIN ISOLATION LEVEL SERIALIZABLE") != nil {
			w.WriteHeader(500)
			return
		}
		began = true
	}
	auth, err := one(c, "auth", r.Header.Get("Authorization"))
	if err != nil {
		w.WriteHeader(500)
		return
	}
	if auth == nil {
		w.WriteHeader(401)
		w.Write([]byte("{}"))
		return
	}
	region := auth.(map[string]any)["region"]
	q := r.URL.Query()
	var result any
	switch r.URL.Path {
	case "/queue":
		name := "queue_typed"
		if q.Get("layout") == "json" {
			name = "queue_json"
		}
		priority, _ := strconv.ParseInt(q.Get("priority"), 10, 64)
		after, _ := strconv.ParseInt(q.Get("after"), 10, 64)
		result, err = one(c, name, region, priority, after)
	case "/detail":
		id, _ := strconv.ParseInt(q.Get("id"), 10, 64)
		result, err = one(c, "detail", id, region)
	case "/action":
		result, err = one(c, "receipt", q.Get("key"))
		id, _ := strconv.ParseInt(q.Get("id"), 10, 64)
		if err == nil && result == nil {
			result, err = one(c, "action", id, region, q.Get("key"))
		}
		if err == nil {
			err = exec("COMMIT")
			began = false
		}
	default:
		w.WriteHeader(404)
		return
	}
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte("{}"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if result == nil {
		w.WriteHeader(404)
		w.Write([]byte("{}"))
		return
	}
	raw, err := json.Marshal(result)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	w.Write(raw)
}
func main() {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Println(runtime.Version())
		return
	}
	raw, err := os.ReadFile("benchmarks/ontology/queries.json")
	if err != nil {
		panic(err)
	}
	if json.Unmarshal(raw, &queries) != nil {
		panic("queries")
	}
	db, err = sql.Open("pgx", os.Getenv("BENCH_DSN")+"&default_query_exec_mode=exec")
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	http.HandleFunc("/", handle)
	fmt.Println("READY")
	if err = http.ListenAndServe("127.0.0.1:"+os.Getenv("PORT"), nil); err != nil {
		panic(err)
	}
}
