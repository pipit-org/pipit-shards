package shards

// Message es la unidad de intercambio de información de los usuarios.
type Message struct {
	ID       int    `json:"id"`
	TopicID  string `json:"topic_id"`
	SenderID string `json:"sender_id"`
	Body     []byte `json:"body"`
}
