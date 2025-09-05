-- Initialize AIPAL database
CREATE DATABASE IF NOT EXISTS platform-core-service;

-- Create extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";