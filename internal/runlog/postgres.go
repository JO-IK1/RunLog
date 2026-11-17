package runlog

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `CREATE TABLE IF NOT EXISTS projects (
 id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS api_keys (
 key_hash BYTEA PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), revoked_at TIMESTAMPTZ);`

// OpenPostgres opens a PostgreSQL pool and verifies connectivity.
func OpenPostgres(ctx context.Context, c Config) (*pgxpool.Pool, error) {
	p, err := pgxpool.New(ctx, c.PG)
	if err != nil {
		return nil, err
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

// Migrate creates the project and API key tables if needed.
func Migrate(ctx context.Context, p *pgxpool.Pool) error {
	_, err := p.Exec(ctx, schema)
	return err
}

func randomID() string {
	return hex.EncodeToString(randomBytes(16))
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

// CreateProject creates a project and returns its ID and API key.
func CreateProject(ctx context.Context, p *pgxpool.Pool, name string) (string, string, error) {
	id := randomID()
	key := "rl_" + hex.EncodeToString(randomBytes(32))
	hash := sha256.Sum256([]byte(key))
	tx, err := p.Begin(ctx)
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "INSERT INTO projects(id,name) VALUES($1,$2)", id, name); err != nil {
		return "", "", err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO api_keys(key_hash,project_id) VALUES($1,$2)", hash[:], id); err != nil {
		return "", "", err
	}
	return id, key, tx.Commit(ctx)
}

// Authenticate returns the project ID associated with an active API key.
func Authenticate(ctx context.Context, p *pgxpool.Pool, key string) (string, error) {
	h := sha256.Sum256([]byte(key))
	var id string
	err := p.QueryRow(ctx, "SELECT project_id FROM api_keys WHERE key_hash=$1 AND revoked_at IS NULL", h[:]).Scan(&id)
	return id, err
}

// Revoke revokes all active API keys for a project.
func Revoke(ctx context.Context, p *pgxpool.Pool, id string) error {
	tag, err := p.Exec(ctx, "UPDATE api_keys SET revoked_at=now() WHERE project_id=$1 AND revoked_at IS NULL", id)
	if err == nil && tag.RowsAffected() == 0 {
		return fmt.Errorf("no active keys for project %s", id)
	}
	return err
}
