#!/bin/bash

set -e
set -x

# Uninstall old versions
for pkg in docker.io docker-doc docker-compose podman-docker containerd runc; do sudo apt-get remove $pkg; done

# Insert gnome-terminal
sudo apt install -y gnome-terminal
# Install Python
sudo apt install python3  python3-pip python3.11-venv
python3 -m venv myenv
. ./myenv/bin/activate

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

# Enable containerd service
sudo systemctl enable kubelet
sudo rm  -f /etc/containerd/config.toml
sudo containerd config default | sudo tee /etc/containerd/config.toml > /dev/null
sudo sed -e 's/SystemdCgroup = false/SystemdCgroup = true/g' -i /etc/containerd/config.toml
sudo systemctl daemon-reload
sudo systemctl restart containerd
sudo systemctl enable containerd 

# Initialize the control plane
POD_NETWORK_CIDR="192.168.0.0/16"
sudo kubeadm init --pod-network-cidr=$POD_NETWORK_CIDR

# Set up kubeconfig for the control plane user
mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config

# Install WeaveNet CNI (you can replace this with Calico or another CNI if needed)
kubectl apply -f https://docs.projectcalico.org/manifests/calico.yaml

# Final message
echo "Control plane setup is complete. Your cluster is ready to accept worker nodes."
kubeadm token create --print-join-command
echo "Remember to save the kubeadm join command generated at the end of the previous command process for worker nodes."