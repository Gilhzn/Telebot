#!/bin/bash
# Stock News Radar — install on an Oracle Cloud (or any Ubuntu) VM as a systemd service.
#
# Use as the "cloud-init script" when creating the instance (Ubuntu 24.04 recommended,
# it ships Python 3.12), or run on an existing VM:  sudo bash oracle-cloud-init.sh
# Re-running the script pulls the latest code from the repo and restarts the service.
#
# Fill in the values below. Secrets stay on the VM (in /opt/stock-news-radar/.env), never in git.
set -euo pipefail

REPO_URL="https://github.com/gilhzn/telebot.git"   # private repo: https://<token>@github.com/gilhzn/telebot.git
BRANCH="main"
TELEGRAM_BOT_TOKEN=""
TELEGRAM_CHAT_ID=""          # empty = send /start to the bot after install
SEC_USER_AGENT=""            # "Full Name email@example.com"
ANTHROPIC_API_KEY=""         # optional

APP_DIR=/opt/stock-news-radar
DATA_DIR=/var/lib/stock-news-radar
SERVICE=stock-news-radar
RUN_USER=radar

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y git python3 python3-venv ca-certificates

if ! python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)'; then
  echo "Python 3.10+ is required (Ubuntu 24.04 has 3.12)." >&2
  exit 1
fi

id -u "$RUN_USER" >/dev/null 2>&1 || useradd --system --home "$APP_DIR" --shell /usr/sbin/nologin "$RUN_USER"

if [ -d "$APP_DIR/.git" ]; then
  git -C "$APP_DIR" fetch --depth 1 origin "$BRANCH"
  git -C "$APP_DIR" reset --hard "origin/$BRANCH"
else
  rm -rf "$APP_DIR"
  git clone --depth 1 --branch "$BRANCH" "$REPO_URL" "$APP_DIR"
fi

python3 -m venv "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/pip" install --quiet --upgrade pip
"$APP_DIR/.venv/bin/pip" install --quiet -r "$APP_DIR/requirements.txt"

mkdir -p "$DATA_DIR"
ENV_FILE="$APP_DIR/.env"
if [ ! -f "$ENV_FILE" ]; then
  cat > "$ENV_FILE" <<EOF
TELEGRAM_BOT_TOKEN=$TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID=$TELEGRAM_CHAT_ID
SEC_USER_AGENT=$SEC_USER_AGENT
ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY
STATE_FILE=$DATA_DIR/state.json
EOF
fi
chmod 600 "$ENV_FILE"
chown -R "$RUN_USER":"$RUN_USER" "$APP_DIR" "$DATA_DIR"

cat > /etc/systemd/system/$SERVICE.service <<EOF
[Unit]
Description=Stock News Radar Telegram bot
After=network-online.target
Wants=network-online.target

[Service]
User=$RUN_USER
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/.venv/bin/python $APP_DIR/bot.py
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE"
systemctl restart "$SERVICE"
echo "Installed. Logs: journalctl -u $SERVICE -f"
