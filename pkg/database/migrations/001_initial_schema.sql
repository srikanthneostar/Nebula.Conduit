-- Migration: 001_initial_schema.sql
-- Description: Initial database schema

-- Enable foreign key support
PRAGMA foreign_keys = ON;

-- Create migrations table to track applied migrations
CREATE TABLE IF NOT EXISTS migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add your initial schema tables below
-- Example:
-- CREATE TABLE IF NOT EXISTS users (
--     id INTEGER PRIMARY KEY AUTOINCREMENT,
--     username TEXT NOT NULL UNIQUE,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- Insert this migration record
INSERT INTO migrations (name) VALUES ('001_initial_schema.sql'); 
