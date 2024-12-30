# Configuring Rebios on Kubernetes
This file describes the steps necessary steps to configure the Rebios on Kubernetes using a EC2 instance

# Install Docker on AWS EC2 instance

## First, update your existing list of packages:
sudo apt update

# Upgrade your system
sudo apt upgrade

## Next, install a few prerequisite packages which let apt use packages over HTTPS:
sudo apt install apt-transport-https ca-certificates curl software-properties-common

## Then add the GPG key for the official Docker repository to your system:
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

## Add the Docker repository to APT sources:
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable"

## Make sure you are about to install from the Docker repo instead of the default Ubuntu repo:
apt-cache policy docker-ce

## Finally, install Docker:
sudo apt install docker-ce

## Check that it’s running:
sudo systemctl status docker

## Executing the Docker Command Without Sudo
sudo usermod -aG docker ${USER}


