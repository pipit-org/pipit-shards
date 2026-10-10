package shards

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
)

const (
	maxBodyBytes = 1 << 20 // 1 MiB por petición de Save
	defaultLimit = 50
	maxLimit     = 200
)

// Shard es la capa de acceso a datos de un nodo.
type Shard struct {
	id string
	tn Tunnel
}

func NewShard(id string, tn Tunnel) *Shard {
	sd := new(Shard)
	sd.id = id
	sd.tn = tn
	return sd
}

func (sd *Shard) ID() string {
	return sd.id
}

func (sd *Shard) Tunnel() Tunnel {
	return sd.tn
}

// ServeHTTP expone las operaciones del Tunnel:
//
//	GET  /ping                          -> 200 si la BD responde, 503 en caso contrario
//	POST /messages                      -> guarda un mensaje (JSON), responde 204
//	GET  /messages?topic=ID&limit=N     -> últimos N mensajes del topic (JSON)
func (sd *Shard) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/ping":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		sd.handlePing(w, r)

	case "/messages":
		switch r.Method {
		case http.MethodPost:
			sd.handleSave(w, r)
		case http.MethodGet:
			sd.handleMessages(w, r)
		default:
			methodNotAllowed(w, "GET, POST")
		}

	default:
		http.NotFound(w, r)
	}
}

func (sd *Shard) handlePing(w http.ResponseWriter, r *http.Request) {
	if err := sd.tn.Ping(r.Context()); err != nil {
		log.Printf("shard: ping falló: %v", err)
		http.Error(w, "base de datos no disponible", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (sd *Shard) handleSave(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	var in Message
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}
	if in.TopicID == "" || in.SenderID == "" {
		http.Error(w, "faltan campos obligatorios: topic_id, sender_id", http.StatusBadRequest)
		return
	}

	err := sd.tn.Save(r.Context(), Message{
		ID:       in.ID,
		TopicID:  in.TopicID,
		SenderID: in.SenderID,
		Body:     in.Body,
	})
	if err != nil {
		log.Printf("shard: save falló: %v", err)
		http.Error(w, "error interno", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (sd *Shard) handleMessages(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "falta el parámetro topic", http.StatusBadRequest)
		return
	}

	limit := defaultLimit
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			http.Error(w, "limit debe ser un entero positivo", http.StatusBadRequest)
			return
		}
		limit = min(n, maxLimit)
	}

	msgs, err := sd.tn.Messages(r.Context(), topic, limit)
	if err != nil {
		log.Printf("shard: messages falló: %v", err)
		http.Error(w, "error interno", http.StatusInternalServerError)
		return
	}

	// Siempre un arreglo JSON, nunca null.
	out := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, Message{
			ID:       m.ID,
			TopicID:  m.TopicID,
			SenderID: m.SenderID,
			Body:     m.Body,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	http.Error(w, "método no permitido", http.StatusMethodNotAllowed)
}
