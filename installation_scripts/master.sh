#!/bin/bash

set -e
set -x

# Allow loopback traffic
sudo iptables -A INPUT -i lo -j ACCEPT
sudo iptables -A OUTPUT -o lo -j ACCEPT
# Allow traffic from the pod network to the main network
sudo iptables -A FORWARD -i cali* -j ACCEPT
sudo iptables -A FORWARD -o cali* -j ACCEPT
sudo iptables -A FORWARD -i cni0 -j ACCEPT
sudo iptables -A FORWARD -o cni0 -j ACCEPT
# Allow incoming traffic on Calico interface
sudo iptables -A INPUT -i cali* -j ACCEPT
sudo iptables -A INPUT -i cni0 -j ACCEPT
# Allow Kubernetes API server
sudo iptables -A INPUT -p tcp --dport 6443 -j ACCEPT
# Allow Kubelet
sudo iptables -A INPUT -p tcp --dport 10250 -j ACCEPT
# Allow Calico BGP
sudo iptables -A INPUT -p tcp --dport 179 -j ACCEPT
# Allow Calico Felix (optional)
sudo iptables -A INPUT -p tcp --dport 9090 -j ACCEPT
# Allow NodePort services
sudo iptables -A INPUT -p tcp --dport 30000:32767 -j ACCEPT
# Save iptables rules (Debian/Ubuntu)
sudo iptables-save | sudo tee /etc/iptables/rules.v4

# Uninstall old versions
for pkg in docker.io docker-doc docker-compose podman-docker containerd runc; do
    if dpkg -l | grep -q "^ii  $pkg "; then
        echo "Removing $pkg..."
        sudo apt-get remove -y $pkg
    else
        echo "$pkg is not installed."
    fi
done

#NTP SERVER
sudo apt install -y ntp
sudo systemctl start ntp
sudo systemctl enable ntp

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