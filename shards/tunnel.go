package shards

import "context"

// Tunnel expone las operaciones disponibles en una shard.
type Tunnel interface {
	// Ping verifica la disponibilidad de la conexión con la base de datos.
	Ping(ctx context.Context) error
	// Save guarda un mensaje en la base de datos.
	Save(context.Context, Message) error
	// Messages consulta los últimos `limit` mensajes.
	Messages(ctx context.Context, topicID string, limit int) ([]Message, error)
}
