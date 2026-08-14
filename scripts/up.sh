#!/usr/bin/env bash
# Setup + start: check prereqs, offer to install/build, then Compose up.
# Usage: scripts/up.sh [-y|--yes] [--rebuild] [docker compose up args...]
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

YES=0
REBUILD=0
COMPOSE_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -y|--yes) YES=1; shift ;;
    --rebuild) REBUILD=1; shift ;;
    --) shift; COMPOSE_ARGS+=("$@"); break ;;
    *) COMPOSE_ARGS+=("$1"); shift ;;
  esac
done

NEED_JAVA=21
NEED_NODE=20

load_env() {
  # shellcheck disable=SC1090
  [[ -s "${NVM_DIR:-$HOME/.nvm}/nvm.sh" ]] && . "${NVM_DIR:-$HOME/.nvm}/nvm.sh"
  # shellcheck disable=SC1090
  [[ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]] && . "$HOME/.sdkman/bin/sdkman-init.sh"
  local jvm
  for jvm in /usr/lib/jvm/java-21-openjdk-* /usr/lib/jvm/temurin-21-*; do
    if [[ -x "$jvm/bin/java" ]]; then
      export JAVA_HOME=$jvm
      export PATH="$JAVA_HOME/bin:$PATH"
      break
    fi
  done
  hash -r 2>/dev/null || true
}

have() { type -P "$1" >/dev/null 2>&1; }

is_wsl() { grep -qi microsoft /proc/version 2>/dev/null; }

is_tty() { [[ -t 0 ]] || [[ -r /dev/tty ]]; }

confirm() {
  local q=$1
  if [[ $YES -eq 1 ]]; then return 0; fi
  if ! is_tty; then
    echo "Non-interactive terminal. Re-run with -y to install/build, or run from a TTY." >&2
    return 1
  fi
  local a=
  read -r -p "$q [y/N] " a </dev/tty || true
  [[ ${a:-} == [yY]* ]]
}

java_major() {
  have java || return 1
  local ver
  ver=$(java -version 2>&1 | grep -oE 'version "[^"]+"' | head -1)
  ver=${ver#version \"}
  ver=${ver%\"}
  if [[ $ver == 1.* ]]; then echo 8; else echo "${ver%%.*}"; fi
}

node_major() {
  have node || return 1
  local ver
  ver=$(node -v)
  ver=${ver#v}
  echo "${ver%%.*}"
}

jar_ready() { compgen -G "$ROOT/backend/target/*.jar" >/dev/null; }

spa_ready() { find "$ROOT/frontend/dist" -name index.html -print -quit 2>/dev/null | grep -q .; }

os_id() {
  [[ -f /etc/os-release ]] || { echo unknown; return; }
  # shellcheck disable=SC1091
  . /etc/os-release
  echo "${ID:-unknown}"
}

need_sudo() {
  have sudo || { echo "sudo is required to install packages." >&2; return 1; }
  sudo -v
}

apt_install() {
  need_sudo
  sudo DEBIAN_FRONTEND=noninteractive apt-get update -y
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y "$@"
}

install_java() {
  case "$(os_id)" in
    ubuntu|debian) apt_install openjdk-21-jdk ;;
    *) echo "Install Java ${NEED_JAVA}+ and re-run (https://adoptium.net/)."; return 1 ;;
  esac
}

install_maven() {
  case "$(os_id)" in
    ubuntu|debian) apt_install maven ;;
    *) echo "Install Maven and re-run (https://maven.apache.org/install.html)."; return 1 ;;
  esac
}

install_node() {
  case "$(os_id)" in
    ubuntu|debian)
      have curl || apt_install curl ca-certificates
      curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
      apt_install nodejs
      ;;
    *) echo "Install Node.js ${NEED_NODE}+ and re-run (https://nodejs.org/)."; return 1 ;;
  esac
}

wait_for_docker() {
  local n=0
  while ! docker info >/dev/null 2>&1; do
    if [[ $n -eq 0 ]]; then
      echo "Docker CLI is present but the daemon is not running."
      if is_wsl; then
        echo "Start Docker Desktop on Windows (WSL integration must include this distro)."
      else
        echo "Start the Docker service, then continue."
      fi
    fi
    confirm "Retry now?" || return 1
    n=$((n + 1))
  done
}

install_docker() {
  if is_wsl; then
    echo "On WSL, use Docker Desktop for Windows, not apt docker."
    echo "Install: https://docs.docker.com/desktop/setup/install/windows-install/"
    echo "Then: Settings → Resources → WSL integration → enable this distro."
    confirm "Retry after Docker Desktop is installed and running?" || return 1
    have docker || { echo "docker still not on PATH."; return 1; }
    wait_for_docker
    return
  fi
  case "$(os_id)" in
    ubuntu|debian)
      apt_install docker.io docker-compose-v2
      sudo usermod -aG docker "$USER" || true
      sudo service docker start || sudo systemctl start docker || true
      if ! docker info >/dev/null 2>&1; then
        echo "Docker installed. If permission denied, log out/in (or: newgrp docker) and re-run."
        return 1
      fi
      ;;
    *) echo "Install Docker Engine + Compose and re-run (https://docs.docker.com/engine/install/)."; return 1 ;;
  esac
}

status_line() {
  printf "  %-12s %s\n" "$1" "$2"
}

check_prereqs() {
  MISSING=()
  echo "Prerequisites"
  local j n
  j=$(java_major || true)
  if [[ -n ${j:-} && $j -ge $NEED_JAVA ]]; then
    status_line "Java" "ok ($j)"
  else
    status_line "Java" "missing (need ${NEED_JAVA}+${j:+; found $j})"
    MISSING+=(java)
  fi
  if have mvn; then
    if [[ -n ${j:-} && $j -ge $NEED_JAVA ]] && mvn -v >/dev/null 2>&1; then
      status_line "Maven" "ok ($(mvn -v | awk '/Apache Maven/ {print $3; exit}'))"
    elif [[ -z ${j:-} || $j -lt $NEED_JAVA ]]; then
      status_line "Maven" "present (needs Java ${NEED_JAVA}+)"
    else
      status_line "Maven" "found but mvn -v failed"
      MISSING+=(maven)
    fi
  else
    status_line "Maven" "missing"
    MISSING+=(maven)
  fi
  n=$(node_major || true)
  if [[ -n ${n:-} && $n -ge $NEED_NODE ]] && have npm; then
    status_line "Node" "ok ($n, npm $(npm -v))"
  else
    status_line "Node" "missing (need ${NEED_NODE}+ with npm${n:+; found $n})"
    MISSING+=(node)
  fi
  if have docker && docker compose version >/dev/null 2>&1; then
    if docker info >/dev/null 2>&1; then
      status_line "Docker" "ok"
    else
      status_line "Docker" "daemon not running"
      MISSING+=(docker-daemon)
    fi
  elif have docker; then
    status_line "Docker" "Compose plugin missing"
    MISSING+=(docker)
  else
    status_line "Docker" "missing"
    MISSING+=(docker)
  fi
}

install_missing() {
  local item
  for item in "${MISSING[@]}"; do
    case "$item" in
      java) install_java ;;
      maven) install_maven ;;
      node) install_node ;;
      docker) install_docker ;;
      docker-daemon) wait_for_docker ;;
    esac
    load_env
  done
}

build_apps() {
  echo "Building backend jar and frontend SPA..."
  mvn -f backend/pom.xml -q -DskipTests package
  npm --prefix frontend ci
  npm --prefix frontend run build
}

load_env
echo "AMN AMRO setup + start"
echo
check_prereqs
echo

if [[ ${#MISSING[@]} -gt 0 ]]; then
  echo "Missing: ${MISSING[*]}"
  confirm "Install / fix the missing prerequisites?" || {
    echo "Aborted. Install the tools above, then re-run."
    exit 1
  }
  install_missing
  echo
  check_prereqs
  echo
  if [[ ${#MISSING[@]} -gt 0 ]]; then
    echo "Still missing: ${MISSING[*]}"
    echo "Fix those, then re-run ./scripts/up.sh"
    exit 1
  fi
fi

need_build=0
if [[ $REBUILD -eq 1 ]]; then
  need_build=1
elif ! jar_ready || ! spa_ready; then
  need_build=1
fi

if [[ $need_build -eq 1 ]]; then
  if jar_ready && spa_ready; then
    confirm "Rebuild backend and frontend?" || need_build=0
  else
    [[ $YES -eq 1 ]] || echo "App artifacts are not built yet (backend/target/*.jar, frontend/dist)."
    confirm "Build them now?" || {
      echo "Aborted. Images copy those paths; nothing to start without them."
      exit 1
    }
  fi
fi

if [[ $need_build -eq 1 ]]; then
  build_apps
else
  echo "Using existing backend jar and frontend dist."
fi

echo
echo "Starting Kafka + backend + frontend."
echo "UI  http://localhost:8081"
echo "API http://localhost:8080"
echo
exec docker compose up --build "${COMPOSE_ARGS[@]+"${COMPOSE_ARGS[@]}"}"
