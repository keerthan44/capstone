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

sudo swapoff -a

sudo kubeadm reset

echo "if this is the master node run"
echo "kubeadm token create --print-join-command"
echo "Run the join command generated from the previous create command if the worker node"
