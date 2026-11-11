package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func proxyTo(raw string) *httputil.ReverseProxy {
	u, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("bad url %q: %v", raw, err)
	}
	return httputil.NewSingleHostReverseProxy(u)
}

func main() {
	monolith := proxyTo(env("MONOLITH_URL", "http://monolith:8080"))
	movies := proxyTo(env("MOVIES_SERVICE_URL", "http://movies-service:8081"))
	events := proxyTo(env("EVENTS_SERVICE_URL", "http://events-service:8082"))

	gradual := env("GRADUAL_MIGRATION", "true") == "true"
	percent, _ := strconv.Atoi(env("MOVIES_MIGRATION_PERCENT", "0"))
	log.Printf("gradual migration: %v, movies percent: %d", gradual, percent)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":true}`))
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/movies"):
			// флаг выключен = миграция movies закончена, весь трафик в новый сервис
			if !gradual || rand.Intn(100) < percent {
				log.Printf("%s %s -> movies-service", r.Method, r.URL.Path)
				movies.ServeHTTP(w, r)
				return
			}
			log.Printf("%s %s -> monolith", r.Method, r.URL.Path)
			monolith.ServeHTTP(w, r)
		case strings.HasPrefix(r.URL.Path, "/api/events"):
			events.ServeHTTP(w, r)
		default:
			monolith.ServeHTTP(w, r)
		}
	})

	port := env("PORT", "8000")
	log.Printf("proxy listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
