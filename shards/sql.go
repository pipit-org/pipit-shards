package shards

const schemaSQL = `
	CREATE TABLE IF NOT EXISTS messages (
		id         INTEGER PRIMARY KEY AUTOINCREMENT,
		topic_id   TEXT NOT NULL,
		sender_id  TEXT NOT NULL,
		body       BLOB NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS messages_topic_id ON messages (topic_id, id);
`

const insertSQL = `
	INSERT INTO messages (topic_id, sender_id, body)
	VALUES (?, ?, ?)
`

// Últimos N mensajes del topic, devueltos en orden cronológico.
const selectMessagesSQL = `
	SELECT id, topic_id, sender_id, body FROM (
		SELECT id, topic_id, sender_id, body
		FROM messages WHERE topic_id = ?
		ORDER BY id DESC LIMIT ?
	) ORDER BY id ASC
`
