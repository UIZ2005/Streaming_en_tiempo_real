#!/bin/bash
sudo apt-get update -y
sudo apt-get install -y curl wget gnupg lsb-release software-properties-common unzip

# Terraform
wget -qO- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor --batch --yes -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list

# Ansible
sudo add-apt-repository --yes ppa:ansible/ansible

sudo apt-get update -y
sudo apt-get install -y terraform ansible

# Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Llave SSH para las VMs de Azure
if [ ! -f /home/vagrant/.ssh/azure_key ]; then
  sudo -u vagrant ssh-keygen -t rsa -b 4096 -f /home/vagrant/.ssh/azure_key -N "" -q
fi

