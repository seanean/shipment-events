-- Runs once on first cluster init (before 01-setup-users.sh).
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS cln;
CREATE SCHEMA IF NOT EXISTS cur;
CREATE SCHEMA IF NOT EXISTS quarantine;
