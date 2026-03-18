#!/bin/bash
# clean-bootstrap-server.sh — Remove Caddy, Redis, and RideChain bootstrap from this VM (Ubuntu).
# Run this to get a clean slate, then run setup-bootstrap-server.sh again (e.g. with bootstrap_binary_url set).
# Usage: sudo bash clean-bootstrap-server.sh
set -e

log() { echo "[clean-bootstrap] $*"; }

log "Stopping and disabling services..."
systemctl stop ridechain-bootstrap.service 2>/dev/null || true
systemctl stop caddy.service 2>/dev/null || true
systemctl stop redis-server.service 2>/dev/null || true
systemctl disable ridechain-bootstrap.service 2>/dev/null || true
systemctl disable caddy.service 2>/dev/null || true
# Redis is often enabled by default; we'll purge it
systemctl disable redis-server.service 2>/dev/null || true

log "Removing systemd unit and data..."
rm -f /etc/systemd/system/ridechain-bootstrap.service
systemctl daemon-reload

rm -rf /opt/ridechain-bootstrap
rm -f /etc/ridechain-bootstrap.env

rm -rf /etc/caddy /var/lib/caddy /var/log/caddy

userdel ridechain 2>/dev/null || true

log "Purging Caddy..."
apt-get purge -y caddy 2>/dev/null || true
rm -f /etc/apt/sources.list.d/caddy-stable.list
rm -f /usr/share/keyrings/caddy-stable-archive-keyring.gpg
apt-get update -qq 2>/dev/null || true

log "Purging Redis..."
apt-get purge -y redis-server 2>/dev/null || true
rm -rf /var/lib/redis /var/log/redis
# Restore default redis.conf if we only removed our lines (optional; purge removes it)
# Leave /etc/redis if present for next install

log "Clean slate done. Run setup-bootstrap-server.sh again (with bootstrap_binary_url in VM metadata if using GCP)."
apt-get autoremove -y -qq 2>/dev/null || true
