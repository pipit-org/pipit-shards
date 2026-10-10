package shards

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"
	"uuid"

	"modernc.org/sqlite"
)

const (
	WriterMaxOpenConns = 1           // No debería haber más de 1 proceso de escritura a la vez
	WriterIdleTimeout  = time.Minute // 1 minuto

	ReaderMaxOpenConns = 128         // Es seguro leer de manera concurrente (muchos "ojos" mirando los mismos datos en memoria)
	ReaderIdleTimeout  = time.Minute // 1 minuto
)

const initSQL = `
	PRAGMA journal_mode = WAL;
	PRAGMA synchronous = NORMAL;
	PRAGMA temp_store = MEMORY;
	PRAGMA busy_timeout = 5000;
	PRAGMA automatic_index = true;
	PRAGMA foreign_keys = ON;
	PRAGMA analysis_limit = 1000;
	PRAGMA trusted_schema = OFF;
`

// ShardManager se encarga de controlar las peticiones a la base de datos.
type ShardManager struct {
	// dsn es el data-source-name o el string que permite conectarse a la base de datos.
	dsn string
	// dbReader es una pool de conexiones de lectura a la base de datos.
	dbReader *sql.DB
	// dbWriter es una pool de conexiones de escritura a la base de datos.
	dbWriter *sql.DB

	shards map[string]*Shard

	mu sync.RWMutex
}

func NewShardManager(dsn string) *ShardManager {
	sm := new(ShardManager)
	sm.dsn = dsn
	sm.shards = make(map[string]*Shard)
	return sm
}

// Init inicializa todo lo necesario para empezar a interactuar con la base de datos y spawnear shards.
func (sm *ShardManager) Init(ctx context.Context) error {
	if sm == nil {
		return ErrNilShardManager
	}

	// cada vez que se abra una conexión ejecutaremos initSQL para propagar nuestra configuración inicial
	sqlite.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
		_, err := conn.ExecContext(ctx, initSQL, nil)
		return err
	})

	writer, err := sql.Open("sqlite", sm.dsn)
	if err != nil {
		return err
	}
	writer.SetMaxOpenConns(WriterMaxOpenConns)
	writer.SetConnMaxIdleTime(WriterIdleTimeout)

	reader, err := sql.Open("sqlite", sm.dsn)
	if err != nil {
		writer.Close()
		return err
	}
	reader.SetMaxOpenConns(ReaderMaxOpenConns)
	reader.SetConnMaxIdleTime(ReaderIdleTimeout)

	sm.mu.Lock()
	sm.dbWriter, sm.dbReader = writer, reader
	sm.mu.Unlock()

	return nil
}

// Migrate crea las tablas si no existen. Llamar después de Init.
func (sm *ShardManager) Migrate(ctx context.Context) error {
	w, _, err := sm.pools()
	if err != nil {
		return err
	}
	_, err = w.ExecContext(ctx, schemaSQL)
	return err
}

// Close optimiza la base de datos y cierra ambas pools. Llamar al apagar el servicio.
func (sm *ShardManager) Close(ctx context.Context) error {
	w, r, err := sm.pools()
	if err != nil {
		return err
	}

	// analysis_limit solo tiene efecto cuando se ejecuta optimize: actualiza las
	// estadísticas que usa el planificador de consultas, acotadas por ese límite.
	_, optErr := w.ExecContext(ctx, "PRAGMA optimize")

	// Tras esto, las llamadas devuelven ErrNotInit en vez de "database is closed".
	sm.mu.Lock()
	sm.dbWriter, sm.dbReader = nil, nil
	sm.mu.Unlock()

	// Al cerrarse la última conexión, SQLite hace checkpoint del WAL a la base principal.
	return errors.Join(optErr, r.Close(), w.Close())
}

// Spawn crea una nueva shard y devuelve la shard, o un error si el manager es nil.
func (sm *ShardManager) Spawn() (*Shard, error) {
	if sm == nil {
		return nil, ErrNilShardManager
	}

	id := uuid.New().String()
	s := NewShard(id, sm)

	sm.mu.Lock()
	sm.shards[id] = s
	sm.mu.Unlock()

	return s, nil
}

func (sm *ShardManager) Shard(id string) (*Shard, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	sd, ok := sm.shards[id]
	if !ok {
		return nil, ErrShardNotFound
	}
	return sd, nil
}

func (sm *ShardManager) Ping(ctx context.Context) error {
	w, r, err := sm.pools()
	if err != nil {
		return err
	}
	if err := w.PingContext(ctx); err != nil {
		return err
	}
	return r.PingContext(ctx)
}

// Save guarda un mensaje. El ID lo asigna la base de datos (autoincrement),
// así que NO es idempotente: guardar dos veces el mismo mensaje crea dos filas.
func (sm *ShardManager) Save(ctx context.Context, m Message) error {
	w, _, err := sm.pools()
	if err != nil {
		return err
	}

	// La columna body es NOT NULL: un cuerpo ausente (nil) se guarda como vacío.
	body := m.Body
	if body == nil {
		body = []byte{}
	}

	_, err = w.ExecContext(ctx, insertSQL, m.TopicID, m.SenderID, body)
	return err
}

// Messages devuelve los últimos `limit` mensajes del topic.
func (sm *ShardManager) Messages(ctx context.Context, topicID string, limit int) ([]Message, error) {
	_, r, err := sm.pools()
	if err != nil {
		return nil, err
	}

	rows, err := r.QueryContext(ctx, selectMessagesSQL, topicID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.TopicID, &m.SenderID, &m.Body); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// pools devuelve (writer, reader) o un error si el manager no está listo.
func (sm *ShardManager) pools() (writer *sql.DB, reader *sql.DB, err error) {
	if sm == nil {
		return nil, nil, ErrNilShardManager
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.dbWriter == nil || sm.dbReader == nil {
		return nil, nil, ErrNotInit
	}
	writer, reader = sm.dbWriter, sm.dbReader

	return writer, reader, nil
}
