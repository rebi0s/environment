# Environment setup

This how-to document shows setep-by-step how to setup the whole environment in a new server.

## Server requirement

The stack of the project requires at lease a server running 4 CPU cores and 16 GB RAM.
For disk, it depends on where to store objects. If using AWS S3 it can be a 50 GB.
But if using Minio in the same server, it must support all the object data in the volume.

The server must be accessible by ssh.

Example:

```
ssh ubuntu@143.106.73.11
```

## Clone this repository to the server

It's recommended to generate a ssh key using `ssh-keygen`, then setup it in github.

Commands to generate ssh key.

```
ssh-keygen
ll
cd .ssh/
ll
cat id_rsa.pub
```

[>> Generating a new SSH key and adding it to the ssh-agent](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)

Create a folder called `/workspace`, give ownership to logged user and clone this repository into it.

```
sudo mkdir -p /workspace
sudo chown $USER /workspace/
cd /workspace
git clone git@github.com:rebi0s/environment.git
```

## Ansible and Docker

There are playbooks in `provisioning` folder to install Docker and MinIO.
So, first of all install Ansible.

```
sudo apt update
sudo apt install -y ansible
```

To install docker run `provisioning/docker.ansible.yaml` playbook.

```
cd provisioning
ansible-playbook docker.ansible.yaml
```

Alternatively you can install docker and docker compose manually.
You can follow [this](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04)
or
[this](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04)
tutorials.

Or just use these commands:

```
sudo apt-get update
sudo apt-get install ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo   "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" |   sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

## MinIO

Use ansible playbook to install minio.

```
cd provisioning
ansible-playbook ubuntu.minio.ansible.yaml
```

MinIO interface can be accessed in [http://<server-ip>:8080](http://<server-ip>:8080)

## Running the containers

Each service has a script in root folder to stop and start the containers
can you can run all of them just by running `start-full-envinronment.sh`.

### Postgres

Iceberg, Hue and Superset requires Postgres database.
A postgres container is created by `iceberg-base/docker-compose.yaml`
and the required databases and users are created by `iceberg-base/postgres.dockerfile`.

Run `start-icebergbase.sh` to start postgres containing the required databases.

### Apache Spark

Apache spark containers (master and workers) are created by `spark/docker-compose.yaml`.

Spark, Iceberg and their dependencies will be installed into the containers by `spark/docker/spark.dockerfile`.
Also scripts to start pyspark, spark-sql and thrift are copied to the root folder of the container.

Run `start-spark.sh` to start Spark master and workers containers.

To access spark container and use pyspark and spark-sql use these commands:

```
docker exec -it spark_master bash
/start-pyspark.sh
/star-spark-sql.sh
```

### MinIO or S3

By default, Spark will use the minio installed by ansible playbook.
But if you prefer to use AWS S3. Just set the variables in `spark/docker/.env` file.

Example:

```
S3_URI=https://s3.amazonaws.com
S3_BUCKET=rebios-test-env
AWS_ACCESS_KEY_ID=key
AWS_SECRET_ACCESS_KEY=secret
AWS_REGION=us-east-2
```

### Hue

Hue container is created by `hue/docker-compose.yaml`.

The connection to Spark is configured in `hue/config/hue.ini#1061`.

A customization we did in this project is a fix to do not have double tables listed in Hue interface.
To do that we removed a "show view" in sql_alchemy since Spark does not have views.

To do that we just replace sql_alchemy.py file when build the hue docker image.

Hue interface can be accessed in [http://<server-ip>:8888/](http://<server-ip>:8888/)

### Superset

#TODO 

Superset interface can be accessed in [http://<server-ip>:8088](http://<server-ip>:8088)

### Airflow

#TODO

### Datahub

#TODO

### Jupyter