package health

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync/atomic"
)

type Checker struct {
	ready atomic.Bool
	alive atomic.Bool
}

func New() *Checker {
	c := &Checker{}
	c.alive.Store(true)
	return c
}

func (c *Checker) SetReady(ready bool) { c.ready.Store(ready) }
func (c *Checker) SetAlive(alive bool) { c.alive.Store(alive) }

func (c *Checker) ServeHTTP(port int) {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if c.alive.Load() {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "alive"})
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "dead"})
		}
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if c.ready.Load() {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "not_ready"})
		}
	})

	go http.ListenAndServe(":"+strconv.Itoa(port), mux)
}
