#!/usr/bin/env bash
#
# Install Docker Engine + Compose plugin on AlmaLinux. Run on BOTH VMs.
# Must be run as root (or with sudo).
#
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then echo "Run as root (sudo)." >&2; exit 1; fi

echo ">> Adding Docker CE repository"
dnf -y install dnf-plugins-core
dnf config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo

echo ">> Installing Docker Engine + Compose plugin"
dnf -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

echo ">> Enabling and starting Docker"
systemctl enable --now docker

echo ">> Versions:"
docker --version
docker compose version

echo ">> Done. (Optional) add your user to the docker group:  usermod -aG docker <user>"
