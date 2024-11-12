# Environment Setup

This how-to document shows step-by-step instructions on how to set up the whole reBI0S environment on a new server.

## Server Requirement

The project stack requires at least a server running 4 CPU cores and 16 GB RAM.
The disk size depends on where objects are stored. If using AWS S3, it can be 100 GB.
However, using Minio on the same server must support all the object data in the volume.

The server must be accessible by SSH.

Example:

```
ssh ubuntu@143.106.73.11
```

## Clone this repository to the server

It's recommended that an SSH key be generated using `ssh-keygen` and then set it up in GitHub.

Commands to generate SSH key.

```
ssh-keygen
ll
cd .ssh/
ll
cat id_rsa.pub
```

[>> Generating a new SSH key and adding it to the ssh-agent](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)

Create a folder called `/workspace`, give the logged-in user ownership, and clone this repository into it.

```
sudo mkdir -p /workspace
sudo chown $USER /workspace/
cd /workspace
git clone git@github.com:rebi0s/environment.git
```

## Ansible and Docker

Playbooks are in the `provisioning` folder for installing Docker and MinIO.
So, first of all, install Ansible.

```
sudo apt update
sudo apt install -y ansible
```

To install docker, run the `provisioning/docker.ansible.yaml` playbook.

```
cd provisioning
ansible-playbook docker.ansible.yaml
```

Alternatively, you can install docker and docker compose manually.
You can follow [this](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04)
or
[this](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04)
tutorials.

Or use these commands:

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
## MinIO or S3

MinIO installation is only required if you are not using any other S3-compatible storage service. 

Use an ansible playbook to install MinIO.

```
cd provisioning
ansible-playbook ubuntu.minio.ansible.yaml
```

MinIO interface can be accessed in [http://<server-ip>:8080](http://<server-ip>:8080)

### S3 Instead

By default, Spark will use the minio installed by the Ansible playbook.
But if you prefer to use AWS S3. Just set the variables in `spark/docker/.env` file.

Example:

```
S3_URI=https://s3.amazonaws.com
S3_BUCKET=rebios-test-env
AWS_ACCESS_KEY_ID=key
AWS_SECRET_ACCESS_KEY=secret
AWS_REGION=us-east-2
```

## Running the Containers

Each service has a script in the root folder to stop and start the containers
and you can run them all just by running `start-full-envinronment.sh`.

### PostgreSQL

Iceberg, Hue, and Superset require a PostgreSQL database.
A PostgreSQL container is created by `iceberg-base/docker-compose.yaml`
and the required databases and users are created by `iceberg-base/postgres.dockerfile`.

Run `start-icebergbase.sh` to start Postgres containing the required databases.

### Apache Spark

Apache spark containers (master and workers) are created by `spark/docker-compose.yaml`.

Spark, Iceberg and their dependencies will be installed into the containers by `spark/docker/spark.dockerfile`.
Also, scripts to start pyspark, spark-sql, and thrift are copied to the container's root folder.

Run `start-spark.sh` to start Spark master and workers containers.

To access the spark container and use pyspark and spark-sql use these commands:

```
docker exec -it spark_master bash
/start-pyspark.sh
/star-spark-sql.sh
```

### Hue

The Hue container is created by `hue/docker-compose.yaml`.

The connection to Spark is configured in `hue/config/hue.ini#1061`.

Our customization in this project is a fix to prevent double tables from being listed in the Hue interface.
We removed a "show view" in sql_alchemy since Spark does not support views.

To do that, we replace the sql_alchemy.py file when building the hue docker image.

Hue interface can be accessed in [http://<server-ip>:8888/](http://<server-ip>:8888/)

### Superset

#TODO 

Superset interface can be accessed in [http://<server-ip>:8088](http://<server-ip>:8088)

### Airflow

#TODO

### Datahub

#TODO

### Jupyter
