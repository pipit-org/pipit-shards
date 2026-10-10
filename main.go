package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"

	"github.com/pipit-org/pipit-shards/shards"
)

var (
	listenFlag string
	dsnFlag    string
)

func main() {
	flag.StringVar(&listenFlag, "listen", ":7000", "dirección donde escucha el servidor")
	flag.StringVar(&dsnFlag, "dsn", "file:pipit.db", "DSN de la base de datos")
	flag.Parse()

	// Init captura este contexto en el hook de conexión, así que debe vivir durante todo el programa
	ctx := context.Background()

	sm := shards.NewShardManager(dsnFlag)
	if err := sm.Init(ctx); err != nil {
		log.Fatalf("no se pudo inicializar el shard manager: %v", err)
	}
	defer sm.Close(ctx)

	if err := sm.Migrate(ctx); err != nil {
		log.Fatalf("no se pudo migrar el esquema: %v", err)
	}

	mux := http.NewServeMux()

	// POST /shards: pide una nueva shard y devuelve su ID.
	mux.HandleFunc("POST /shards", func(w http.ResponseWriter, r *http.Request) {
		sd, err := sm.Spawn()
		if err != nil {
			log.Printf("spawn falló: %v", err)
			http.Error(w, "error interno", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": sd.ID()})
	})

	// /shards/{id}/...: delega en la shard (ping, save, messages).
	mux.HandleFunc("/shards/{id}/", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")

		sd, err := sm.Shard(id)
		if err != nil {
			http.NotFound(w, r)
			return
		}

		// La shard espera rutas relativas (/ping, /messages).
		http.StripPrefix("/shards/"+id, sd).ServeHTTP(w, r)
	})

	// GET /health: estado de la base de datos
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if err := sm.Ping(r.Context()); err != nil {
			http.Error(w, "base de datos no disponible", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("Shard manager escuchando en %s", listenFlag)
	log.Fatal(http.ListenAndServe(listenFlag, mux))
}
