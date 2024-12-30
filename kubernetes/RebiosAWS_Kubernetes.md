# Configuring Rebios on Kubernetes
This file describes the steps necessary steps to configure the Rebios on Kubernetes using a EC2 instance

# reBI0S Architecture

The rebI0S architecture is based on the state of the practice in Big Data architectures. 

![The reBI0S Architecture](reBI0SArch.png "The reBI0S Architecture")

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


# Install Microk8s on AWS
### install the MicroK8s using the following command
sudo snap install microk8s --classic

### Next, you can check the status of the MicroK8s cluster by running the following command.
sudo microk8s status

### To avoid using microk8s as a prefix while running kubectl commands, you can add an alias of yuor preference if you don’t have an existing installation of kubectl.
### In this installation will be used "kc" as alias using the following command;
alias kc='sudo microk8s kubectl'

### Now, you can execute kubectl commands directly without the prefix.
kc get nodes

### In case you want to use native kubectl for executing the commands, copy the MicroK8s generated kubeconfig to the ~/.kube/config file by using the following command
mkdir ~/.kube
sudo microk8s kubectl config view --raw > ~/.kube/config

### Now, you can use the native kubectl as well to run the commands.
kc get pods -A

### Add user to microk8s group
sudo usermod -a -G microk8s ubuntu
sudo chown -R ubuntu ~/.kube


# AWS Architecture

The rebI0S architecture on AWS is comprised of the following components distributed on two EC2 instances. 

![The reBI0S Architecture on AWS](ArchAWS.jpg "The reBI0S Architecture on AWS")


