-- Users table
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tasks table
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    script TEXT NOT NULL,
    full_path TEXT NOT NULL,
    args TEXT, -- JSON array
    env TEXT,  -- JSON array
    status TEXT NOT NULL,
    output TEXT,
    error TEXT,
    exit_code INTEGER,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    created_by INTEGER NOT NULL,
    FOREIGN KEY(created_by) REFERENCES users(id)
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_tasks_created_by ON tasks(created_by);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
