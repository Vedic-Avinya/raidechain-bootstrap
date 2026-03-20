package persist

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "modernc.org/sqlite"
)

// UserRecord is the permanent record of a registered user.
type UserRecord struct {
	PeerID      string `json:"peerId"`
	DeviceID    string `json:"deviceId,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
	City        string `json:"city,omitempty"`
	Lat         float64 `json:"lat,omitempty"`
	Lng         float64 `json:"lng,omitempty"`
	CreatedAt   int64  `json:"createdAt"`
	UpdatedAt   int64  `json:"updatedAt"`
}

// Store is a lightweight SQLite-backed persistent store for permanent user records.
type Store struct {
	db *sql.DB
}

// New opens (or creates) the SQLite database at the given path and ensures the schema exists.
func New(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("persist: open %s: %w", dbPath, err)
	}
	// WAL mode for better concurrent read performance
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("persist: WAL pragma: %w", err)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("persist: migrate: %w", err)
	}
	slog.Info("persist_store_ready", "path", dbPath)
	return &Store{db: db}, nil
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			peer_id      TEXT PRIMARY KEY,
			device_id    TEXT DEFAULT '',
			display_name TEXT DEFAULT '',
			city         TEXT DEFAULT '',
			lat          REAL DEFAULT 0,
			lng          REAL DEFAULT 0,
			created_at   INTEGER NOT NULL,
			updated_at   INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_users_device_id ON users(device_id) WHERE device_id != '';
		CREATE INDEX IF NOT EXISTS idx_users_city ON users(city) WHERE city != '';
	`)
	return err
}

// Upsert creates or updates a user record. On first register, createdAt is set; on subsequent, only updatedAt changes.
func (s *Store) Upsert(ctx context.Context, rec UserRecord) error {
	now := time.Now().Unix()
	if rec.UpdatedAt == 0 {
		rec.UpdatedAt = now
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO users (peer_id, device_id, display_name, city, lat, lng, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(peer_id) DO UPDATE SET
			device_id    = CASE WHEN excluded.device_id != '' THEN excluded.device_id ELSE users.device_id END,
			display_name = CASE WHEN excluded.display_name != '' THEN excluded.display_name ELSE users.display_name END,
			city         = CASE WHEN excluded.city != '' THEN excluded.city ELSE users.city END,
			lat          = CASE WHEN excluded.lat != 0 THEN excluded.lat ELSE users.lat END,
			lng          = CASE WHEN excluded.lng != 0 THEN excluded.lng ELSE users.lng END,
			updated_at   = excluded.updated_at
	`, rec.PeerID, rec.DeviceID, rec.DisplayName, rec.City, rec.Lat, rec.Lng, now, rec.UpdatedAt)
	return err
}

// GetByPeerID returns a user record by peerId.
func (s *Store) GetByPeerID(ctx context.Context, peerID string) (*UserRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT peer_id, device_id, display_name, city, lat, lng, created_at, updated_at FROM users WHERE peer_id = ?`, peerID)
	return scanUser(row)
}

// GetByDeviceID returns a user record by deviceId (for peer recovery after reinstall).
func (s *Store) GetByDeviceID(ctx context.Context, deviceID string) (*UserRecord, error) {
	if deviceID == "" {
		return nil, nil
	}
	row := s.db.QueryRowContext(ctx, `SELECT peer_id, device_id, display_name, city, lat, lng, created_at, updated_at FROM users WHERE device_id = ? ORDER BY updated_at DESC LIMIT 1`, deviceID)
	return scanUser(row)
}

// TotalUsers returns the total number of registered users ever.
func (s *Store) TotalUsers(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&count)
	return count, err
}

// Stats returns aggregate stats: total users, active (updated in last 30 days), cities.
type Stats struct {
	TotalUsers  int64 `json:"totalUsers"`
	ActiveUsers int64 `json:"activeUsers"`
	Cities      int64 `json:"cities"`
}

func (s *Store) Stats(ctx context.Context) (*Stats, error) {
	var st Stats
	cutoff := time.Now().Unix() - 30*24*3600
	err := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(CASE WHEN updated_at >= ? THEN 1 END),
			COUNT(DISTINCT CASE WHEN city != '' THEN city END)
		FROM users
	`, cutoff).Scan(&st.TotalUsers, &st.ActiveUsers, &st.Cities)
	if err != nil {
		return nil, err
	}
	return &st, nil
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

func scanUser(row *sql.Row) (*UserRecord, error) {
	var u UserRecord
	err := row.Scan(&u.PeerID, &u.DeviceID, &u.DisplayName, &u.City, &u.Lat, &u.Lng, &u.CreatedAt, &u.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}
