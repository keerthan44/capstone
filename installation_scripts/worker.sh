#!/bin/bash

set -e
set -x

# Uninstall old versions
for pkg in docker.io docker-doc docker-compose podman-docker containerd runc; do
    if dpkg -l | grep -q "^ii  $pkg "; then
        echo "Removing $pkg..."
        sudo apt-get remove -y $pkg
    else
        echo "$pkg is not installed."
    fi
done

# Insert gnome-terminal
sudo apt install -y gnome-terminal curl

# Install docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo apt-get install -y uidmap
dockerd-rootless-setuptool.sh install
export DOCKER_HOST=unix:///run/user/1000/docker.sock

# Add Kubernetes apt repository
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.31/deb/Release.key | sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
sudo rm -f /etc/apt/sources.list.d/kubernetes.list
echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.31/deb/ /' | sudo tee /etc/apt/sources.list.d/kubernetes.list

# Install kubeadm, kubelet, and kubectl
sudo apt-get update
sudo apt-get install -y kubelet kubeadm kubectl
sudo apt-mark hold kubelet kubeadm kubectl

# Enable kubelet service
sudo systemctl enable kubelet
sudo rm  -f /etc/containerd/config.toml
sudo containerd config default | sudo tee /etc/containerd/config.toml > /dev/null
sudo sed -e 's/SystemdCgroup = false/SystemdCgroup = true/g' -i /etc/containerd/config.toml
sudo systemctl daemon-reload
sudo systemctl restart containerd
sudo systemctl enable containerd 


echo "Enter the kubeadm join command with sudo to connect this worker node to cluster"