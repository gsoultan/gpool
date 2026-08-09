#!/usr/bin/env bash
# Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.
#
# Bring up every database gpool's integration tests need, using Apple's
# `container` runtime.
#
# gpool's unit tests need nothing. Its integration tests need a real server, and
# several classes of bug in this repository were invisible without one: a
# released-but-unscanned row leaving a connection busy, DISCARD ALL invalidating
# pgx's statement cache, ClickHouse returning uint8 where the driver's documented
# type set says it cannot. This script is what makes running them cheap enough
# that they actually get run.
#
#   ./.junie/scripts/testdbs.sh up            # start everything
#   ./.junie/scripts/testdbs.sh up mysql      # or just one
#   eval "$(./.junie/scripts/testdbs.sh env)" # export the DSNs
#   ./.junie/scripts/testdbs.sh status
#   ./.junie/scripts/testdbs.sh down
#
# Each engine is configured for what the tests actually exercise, which is more
# than a default image gives you: PostgreSQL needs wal_level=logical before it
# will hand out a replication slot, and MySQL and MariaDB need row-format binary
# logging before there is anything for CDC to read.

set -euo pipefail

readonly PREFIX="gpool"
readonly PG_PASSWORD="postgres"
readonly MY_PASSWORD="root"
readonly CH_PASSWORD="clickhouse"
# SQL Server refuses to start on a password it considers weak, and says so only
# in the log, several seconds after appearing to launch cleanly.
readonly MSSQL_PASSWORD='Str0ng!Passw0rd'
# sp_cdc_enable_db refuses to run on master, so CDC tests need a database of
# their own with capture switched on.
readonly MSSQL_CDC_DB="gpoolcdc"

readonly ALL_ENGINES=(postgres mysql mariadb clickhouse mssql)

die() { printf '%s\n' "$*" >&2; exit 1; }
note() { printf '  %s\n' "$*"; }

# --- container runtime --------------------------------------------------------
# Apple's `container` locally, Docker in CI. They differ in three places that
# matter here — how memory is spelled, how a platform is selected, and how
# running containers are listed — so those are wrapped rather than sprinkled
# through the engine definitions.
if command -v container >/dev/null 2>&1 && container system status >/dev/null 2>&1; then
  readonly RUNTIME="container"
elif command -v docker >/dev/null 2>&1; then
  readonly RUNTIME="docker"
else
  die "no container runtime found: install Apple's 'container' or Docker"
fi

run_container() { "${RUNTIME}" run "$@"; }
exec_container() { "${RUNTIME}" exec "$@"; }

# running lists the names of running containers, one per line.
running() {
  if [ "${RUNTIME}" = container ]; then
    container ls 2>/dev/null | awk 'NR>1 {print $1}'
  else
    docker ps --format '{{.Names}}' 2>/dev/null
  fi
}

is_running() { running | grep -qx "$1"; }

# amd64_flags selects an x86-64 image. Only SQL Server needs it, and only on an
# arm64 host — a CI runner is already x86-64, where forcing the platform is at
# best redundant and at worst refused.
amd64_flags() {
  [ "$(uname -m)" = "x86_64" ] && return 0
  if [ "${RUNTIME}" = container ]; then
    printf -- '--arch amd64 -m 4G -c 4'
  else
    printf -- '--platform linux/amd64 --memory 4g --cpus 4'
  fi
}

# --- per-engine settings ------------------------------------------------------
# Ports are offset well clear of a local install's defaults, so a running
# PostgreSQL on 5432 is never what a test accidentally connects to.

image_of() {
  case "$1" in
    postgres)   echo "docker.io/library/postgres:17-alpine" ;;
    mysql)      echo "docker.io/library/mysql:8.4" ;;
    mariadb)    echo "docker.io/library/mariadb:11" ;;
    clickhouse) echo "docker.io/clickhouse/clickhouse-server:24-alpine" ;;
    mssql)      echo "mcr.microsoft.com/mssql/server:2022-latest" ;;
  esac
}

# port_of prints the host port an engine is published on.
#
# Overridable per engine, because these are host ports on a developer's machine
# and nothing reserves them. A laptop running another project's containers can
# already own one, and the failure that produces — a container that will not bind
# — reads as an unrelated problem several steps later.
port_of() {
  case "$1" in
    postgres)   echo "${GPOOL_POSTGRES_PORT:-55432}" ;;
    mysql)      echo "${GPOOL_MYSQL_PORT:-53306}" ;;
    mariadb)    echo "${GPOOL_MARIADB_PORT:-53307}" ;;
    clickhouse) echo "${GPOOL_CLICKHOUSE_PORT:-59000}" ;;
    mssql)      echo "${GPOOL_MSSQL_PORT:-51433}" ;;
  esac
}

# dsn_of prints the environment variable name and value the tests read.
dsn_of() {
  local port; port="$(port_of "$1")"
  case "$1" in
    postgres)   echo "DATABASE_URL=postgres://postgres:${PG_PASSWORD}@127.0.0.1:${port}/postgres?sslmode=disable" ;;
    mysql)      echo "MYSQL_DSN=root:${MY_PASSWORD}@tcp(127.0.0.1:${port})/gpool?parseTime=true" ;;
    mariadb)    echo "MARIADB_DSN=root:${MY_PASSWORD}@tcp(127.0.0.1:${port})/gpool?parseTime=true" ;;
    clickhouse) echo "CLICKHOUSE_DSN=clickhouse://default:${CH_PASSWORD}@127.0.0.1:${port}/gpool" ;;
    mssql)
      echo "MSSQL_DSN=sqlserver://sa:${MSSQL_PASSWORD}@127.0.0.1:${port}?database=master"
      # CDC cannot be enabled on a system database, so its tests get their own.
      echo "MSSQL_CDC_DSN=sqlserver://sa:${MSSQL_PASSWORD}@127.0.0.1:${port}?database=${MSSQL_CDC_DB}"
      ;;
  esac
}

start_engine() {
  local engine="$1" name="${PREFIX}-$1" port image
  port="$(port_of "$engine")"
  image="$(image_of "$engine")"

  if is_running "${name}"; then
    note "${engine}: already running"
    return 0
  fi
  "${RUNTIME}" rm -f "${name}" >/dev/null 2>&1 || true

  case "$engine" in
    postgres)
      # wal_level=logical is what makes a replication slot possible at all; the
      # CDC tests skip themselves without it rather than fail confusingly.
      run_container -d --rm --name "${name}" -p "${port}:5432" \
        -e POSTGRES_PASSWORD="${PG_PASSWORD}" \
        "${image}" \
        -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10 \
        -c max_connections=400 >/dev/null
      ;;
    mysql)
      # ROW format carries the before-image CDC needs; GTIDs make a recorded
      # position survive a failover, which a file offset does not.
      run_container -d --rm --name "${name}" -p "${port}:3306" \
        -e MYSQL_ROOT_PASSWORD="${MY_PASSWORD}" -e MYSQL_DATABASE=gpool \
        "${image}" \
        --log-bin=mysql-bin --binlog-format=ROW --server-id=1 \
        --gtid-mode=ON --enforce-gtid-consistency=ON >/dev/null
      ;;
    mariadb)
      # MariaDB's GTIDs are always on and are written differently from MySQL's,
      # which is exactly why the CDC vendor has a separate flavour.
      run_container -d --rm --name "${name}" -p "${port}:3306" \
        -e MARIADB_ROOT_PASSWORD="${MY_PASSWORD}" -e MARIADB_DATABASE=gpool \
        "${image}" \
        --log-bin=mariadb-bin --binlog-format=ROW --server-id=1 >/dev/null
      ;;
    clickhouse)
      run_container -d --rm --name "${name}" -p "${port}:9000" \
        -e CLICKHOUSE_PASSWORD="${CH_PASSWORD}" -e CLICKHOUSE_DB=gpool \
        -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
        "${image}" >/dev/null
      ;;
    mssql)
      # The Agent is what runs the CDC capture job. Without it sp_cdc_enable_table
      # succeeds, the change tables are created, and they stay empty forever.
      #
      # Microsoft publishes SQL Server for amd64 only. On Apple silicon this
      # needs emulation, and whether that works is a property of the host rather
      # than of gpool — so a failure here is reported plainly instead of being
      # retried into a confusing timeout.
      run_container -d --rm --name "${name}" -p "${port}:1433" \
        $(amd64_flags) \
        -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD="${MSSQL_PASSWORD}" -e MSSQL_PID=Developer \
        -e MSSQL_AGENT_ENABLED=true \
        "${image}" >/dev/null 2>&1 || {
          note "mssql: could not start (amd64 image on $(uname -m) host)"
          return 1
        }
      ;;
  esac
  note "${engine}: starting on 127.0.0.1:${port}"
}

# ready_engine asks the engine itself, using the client already inside its image,
# so the host needs no psql/mysql/sqlcmd installed.
ready_engine() {
  local engine="$1" name="${PREFIX}-$1"
  case "$engine" in
    postgres)   exec_container "${name}" pg_isready -q ;;
    mysql)      exec_container "${name}" mysqladmin ping -uroot -p"${MY_PASSWORD}" --silent ;;
    mariadb)    exec_container "${name}" mariadb-admin ping -uroot -p"${MY_PASSWORD}" --silent ;;
    clickhouse) exec_container "${name}" clickhouse-client --password "${CH_PASSWORD}" --query "SELECT 1" ;;
    mssql)      exec_container "${name}" /opt/mssql-tools18/bin/sqlcmd -C \
                  -S localhost -U sa -P "${MSSQL_PASSWORD}" -Q "SELECT 1" ;;
  esac >/dev/null 2>&1
}

# provision runs the one-off setup an engine needs after it is up.
provision() {
  [ "$1" = mssql ] || return 0

  exec_container "${PREFIX}-mssql" /opt/mssql-tools18/bin/sqlcmd -C \
    -S localhost -U sa -P "${MSSQL_PASSWORD}" -Q "
IF DB_ID('${MSSQL_CDC_DB}') IS NULL CREATE DATABASE ${MSSQL_CDC_DB};" >/dev/null 2>&1 || return 1

  exec_container "${PREFIX}-mssql" /opt/mssql-tools18/bin/sqlcmd -C \
    -S localhost -U sa -P "${MSSQL_PASSWORD}" -d "${MSSQL_CDC_DB}" -Q "
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = '${MSSQL_CDC_DB}') = 0
  EXEC sys.sp_cdc_enable_db;" >/dev/null 2>&1 || return 1

  note "mssql: ${MSSQL_CDC_DB} database created with CDC enabled"
}

wait_ready() {
  local engine="$1" tries="${2:-90}"
  for _ in $(seq 1 "${tries}"); do
    if ready_engine "${engine}"; then
      note "${engine}: ready"
      provision "${engine}" || note "${engine}: provisioning failed"
      return 0
    fi
    if ! is_running "${PREFIX}-${engine}"; then
      note "${engine}: container exited — check: container logs ${PREFIX}-${engine}"
      return 1
    fi
    sleep 2
  done
  note "${engine}: not ready after $((tries * 2))s"
  return 1
}

cmd_up() {
  local engines=("$@")
  [ ${#engines[@]} -eq 0 ] && engines=("${ALL_ENGINES[@]}")

  # Started together, waited on afterwards: pulling five images serially is the
  # slow part, and none of them depend on another.
  local started=()
  for engine in "${engines[@]}"; do
    start_engine "${engine}" && started+=("${engine}") || true
  done

  local failed=0
  for engine in "${started[@]}"; do
    wait_ready "${engine}" || failed=1
  done

  echo
  echo "Export the connection strings with:"
  echo "  eval \"\$(${BASH_SOURCE[0]} env)\""
  return "${failed}"
}

cmd_down() {
  local engines=("$@")
  [ ${#engines[@]} -eq 0 ] && engines=("${ALL_ENGINES[@]}")
  for engine in "${engines[@]}"; do
    "${RUNTIME}" stop "${PREFIX}-${engine}" >/dev/null 2>&1 && note "${engine}: stopped" || true
    "${RUNTIME}" rm -f "${PREFIX}-${engine}" >/dev/null 2>&1 || true
  done
}

# cmd_env prints only what is actually reachable, so a partial bring-up does not
# hand the test suite a DSN pointing at nothing.
cmd_env() {
  for engine in "${ALL_ENGINES[@]}"; do
    if ready_engine "${engine}"; then
      # An engine may declare more than one variable, so each line is exported
      # rather than only the first.
      # Emitted as NAME='value'. A MySQL DSN contains tcp(host:port), and
      # unquoted parentheses are a syntax error in bash — which zsh tolerates, so
      # this only shows up somewhere else, such as CI.
      dsn_of "${engine}" | while IFS= read -r setting; do
        [ -n "${setting}" ] || continue
        echo "export ${setting%%=*}='${setting#*=}'"
      done
    fi
  done
}

cmd_status() {
  printf '%-12s %-9s %-8s %s\n' ENGINE STATE READY ADDRESS
  for engine in "${ALL_ENGINES[@]}"; do
    local state=stopped ready=no
    if is_running "${PREFIX}-${engine}"; then
      state=running
      ready_engine "${engine}" && ready=yes
    fi
    printf '%-12s %-9s %-8s 127.0.0.1:%s\n' "${engine}" "${state}" "${ready}" "$(port_of "${engine}")"
  done
}

case "${1:-}" in
  up)     shift; cmd_up "$@" ;;
  down)   shift; cmd_down "$@" ;;
  env)    shift; cmd_env "$@" ;;
  status) cmd_status ;;
  *)      die "usage: $(basename "$0") up|down|env|status [engine...]
engines: ${ALL_ENGINES[*]}" ;;
esac
