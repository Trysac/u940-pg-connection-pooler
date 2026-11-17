#!/usr/bin/env bash
set -euo pipefail

sudo systemctl start postgresql.service

HBA_FILE=$(sudo -u postgres psql -t -P format=unaligned -c 'SHOW hba_file;')
sudo sed -i '1i host all all 127.0.0.1/32 trust' "$HBA_FILE"
sudo sed -i '1i host all all ::1/128 trust' "$HBA_FILE"
sudo systemctl reload postgresql.service

sudo -u postgres psql -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'bench') THEN
        CREATE ROLE bench WITH LOGIN SUPERUSER PASSWORD 'pw';
    END IF;
END
$$;
SQL

sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = 'app'" | grep -q 1 || \
    sudo -u postgres createdb -O bench app
