# Configuring Rebios on Kubernetes
This file describes the steps necessary steps to configure the Rebios on Kubernetes using a EC2 instance

# reBI0S Architecture

The rebI0S architecture is based on the state of the practice in Big Data architectures. 

![The reBI0S Architecture](reBI0SArch.png "The reBI0S Architecture")

# Install Docker on AWS EC2 instance

## First, update your existing list of packages:
`sudo apt update

# Upgrade your system
```
	sudo apt upgrade
```

## Next, install a few prerequisite packages which let apt use packages over HTTPS:
```
	sudo apt install apt-transport-https ca-certificates curl software-properties-common
```	


## Then add the GPG key for the official Docker repository to your system:
```
	curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
```

## Add the Docker repository to APT sources:
``` 
	sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable"
```

## Make sure you are about to install from the Docker repo instead of the default Ubuntu repo:
``` 
	apt-cache policy docker-ce
```	

## Finally, install Docker:
```
	sudo apt install docker-ce
```

## Check that it’s running:
```
	sudo systemctl status docker
```	

## Executing the Docker Command Without Sudo
```
	sudo usermod -aG docker ${USER}
```


# Install Microk8s on AWS
### install the MicroK8s using the following command
```
	sudo snap install microk8s --classic
```

### Next, you can check the status of the MicroK8s cluster by running the following command.
``` 
	sudo microk8s status
```

### To avoid using microk8s as a prefix while running kubectl commands, you can add an alias of yuor preference if you don’t have an existing installation of kubectl.
### In this installation will be used "kc" as alias using the following command;
```
	alias kc='sudo microk8s kubectl'
```	

### Now, you can execute kubectl commands directly without the prefix.
```
	kc get nodes
```

### In case you want to use native kubectl for executing the commands, copy the MicroK8s generated kubeconfig to the ~/.kube/config file by using the following command
```
	mkdir ~/.kube
	sudo microk8s kubectl config view --raw > ~/.kube/config
```

### Now, you can use the native kubectl as well to run the commands.
```
	kc get pods -A
```

### Add user to microk8s group
```
	sudo usermod -a -G microk8s ubuntu
	sudo chown -R ubuntu ~/.kube
```


# AWS Architecture

The rebI0S architecture on AWS is comprised of the following components distributed on two EC2 instances. 

![The reBI0S Architecture on AWS](ArchAWS.jpg "The reBI0S Architecture on AWS")

# Deploy Postgres on Kubernetes

### Create namespace
```
	kc delete namespace rebios-postgres 
	kc create namespace rebios-postgres 
```

### Create Passwords
```
echo -n 'postgres' | base64
echo -n 'admin123' | base64
```


### Create Secret
file: psql-secret.yaml

```
	apiVersion: v1
	kind: Secret
	metadata:
	  name: psql-secret
	type: Opaque
	data:
	  password: cG9zdGdyZXM=
	  repmgr-password: cG9zdGdyZXM=
	  admin-password: cG9zdGdyZXM=
```

### Apply Config Map
```
	kc apply -n rebios-postgres -f psql-secret.yaml
	kc get secret 
```

### Create Volume
file: psql-pv.yaml

```
apiVersion: v1
kind: PersistentVolume
metadata:
  name: postgres-volume-0
  # name: rebios-postgresql-0
  labels:
    app: postgres-app
spec:
  storageClassName: psql-manual
  claimRef:
    name: rebios-postgresql-0
    namespace: rebios-postgres
  capacity:
    storage: 8Gi
  accessModes:
    - ReadWriteMany
  hostPath:
    path: /data/postgres/postgres0

---

apiVersion: v1
kind: PersistentVolume
metadata:
  name: postgres-volume-1
  # name: rebios-postgresql-1
  labels:
    app: postgres-app
spec:
  storageClassName: psql-manual
  claimRef:
    name: rebios-postgresql-1
    namespace: rebios-postgres
  capacity:
    storage: 8Gi
  accessModes:
    - ReadWriteMany
  hostPath:
    path: /data/postgres/postgres1
```

### Apply Volume
```
	kc apply -n rebios-postgres -f psql-pv.yaml
	kc get pv
```

### Create Volume Claim
file: psql-pvclaim.yaml

```
	apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: rebios-postgresql-0
  labels:
    app: postgres-app
spec:
  storageClassName: psql-manual
  volumeName: postgres-volume-0
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 8Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: rebios-postgresql-1
  labels:
    app: postgres-app
spec:
  storageClassName: psql-manual
  volumeName: postgres-volume-1
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 8Gi
```

### Apply Volume Claim
```
	kc apply -n rebios-postgres -f psql-pvclaim.yaml
	kc -n rebios-postgres get pvc
```

### Create Config
file: config.yaml

```
	service:
	  type: LoadBalancer
	  ports:
		postgresql: 5433
		nodePort: 30028
```

### Install Postgres
```
  helm upgrade --cleanup-on-fail \
  --namespace rebios-postgres \
  --install rebios-postgres oci://registry-1.docker.io/bitnamicharts/postgresql-ha \
  --values config.yaml
```

### Install Output
```
Pulled: registry-1.docker.io/bitnamicharts/postgresql-ha:15.1.4
Digest: sha256:541dc2193dbfd6af5af7b614c5882e30aef54072539197cbc20f6951ff667334
NAME: rebios-postgres
LAST DEPLOYED: Wed Jan  1 12:22:09 2025
NAMESPACE: rebios-postgres
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
CHART NAME: postgresql-ha
CHART VERSION: 15.1.4
APP VERSION: 17.2.0

Did you know there are enterprise versions of the Bitnami catalog? For enhanced secure software supply chain features, unlimited pulls from Docker, LTS support, or application customization, see Bitnami Premium or Tanzu Application Catalog. See https://www.arrow.com/globalecs/na/vendors/bitnami for more information.
** Please be patient while the chart is being deployed **
PostgreSQL can be accessed through Pgpool via port 5432 on the following DNS name from within your cluster:

    rebios-postgres-postgresql-ha-pgpool.rebios-postgres.svc.cluster.local

Pgpool acts as a load balancer for PostgreSQL and forward read/write connections to the primary node while read-only connections are forwarded to standby nodes.

To get the password for "postgres" run:

    export POSTGRES_PASSWORD=$(kubectl get secret --namespace rebios-postgres rebios-postgres-postgresql-ha-postgresql -o jsonpath="{.data.password}" | base64 -d)

To get the password for "repmgr" run:

    export REPMGR_PASSWORD=$(kubectl get secret --namespace rebios-postgres rebios-postgres-postgresql-ha-postgresql -o jsonpath="{.data.repmgr-password}" | base64 -d)

To connect to your database run the following command:

    kubectl run rebios-postgres-postgresql-ha-client --rm --tty -i --restart='Never' --namespace rebios-postgres --image docker.io/bitnami/postgresql-repmgr:17.2.0-debian-12-r6 --env="PGPASSWORD=$POSTGRES_PASSWORD"  \
        --command -- psql -h rebios-postgres-postgresql-ha-pgpool -p 5432 -U postgres -d postgres

To connect to your database from outside the cluster execute the following commands:

    kubectl port-forward --namespace rebios-postgres svc/rebios-postgres-postgresql-ha-pgpool 5432:5432 &
    psql -h 127.0.0.1 -p 5432 -U postgres -d postgres

WARNING: There are "resources" sections in the chart not set. Using "resourcesPreset" is not recommended for production. For production installations, please set the following values according to your workload needs:
  - pgpool.resources
  - postgresql.resources
  - witness.resources
+info https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/

```



### Check Postgres pods
```
	kc get pods -n rebios-postgres
```
### Pods Output
```
rebios-postgres-postgresql-ha-pgpool-6c8f56fbbf-npkx8   0/1     Running   1 (14s ago)   95s
rebios-postgres-postgresql-ha-postgresql-0              0/1     Pending   0             95s
rebios-postgres-postgresql-ha-postgresql-1              0/1     Pending   0             95s
```

# Create PgAdmin
file: pgadmin-secret.yaml

```
	apiVersion: v1
	kind: Secret
	metadata:
	  name: pgadmin-secret
	type: Opaque
	data:
	  pgadmin-default-password: YWRtaW4xMjM=``` 

### Apply Secret
```
	kc apply -n rebios-postgres -f pgadmin-secret.yaml
	kc  -n rebios-postgres get secrets
```


### Create Depolyment
file: pgadmin-deployment.yaml
```
	apiVersion: apps/v1
	kind: Deployment
	metadata:
	  name: pgadmin
	spec:
	  selector:
	   matchLabels:
		app: pgadmin
	  replicas: 1
	  template:
		metadata:
		  labels:
			app: pgadmin
		spec:
		  containers:
			- name: pgadmin4
			  image: dpage/pgadmin4
			  env:
				- name: PGADMIN_DEFAULT_EMAIL
				  value: "admin@admin.com"
				- name: PGADMIN_DEFAULT_PASSWORD
				  valueFrom:
					secretKeyRef:
					  name: pgadmin-secret
					  key: pgadmin-default-password
				- name: PGADMIN_PORT
				  value: "80"
			  ports:
				- containerPort: 80
				  name: pgadminport
```

### Apply Deployment
```
	kc apply -n rebios-postgres -f pgadmin-deployment.yaml
	kc  -n rebios-postgres get deployment
```


### Connect to POD
```
	kc get pods -n rebios-postgres
	kc exec -it -n rebios-postgres rebios-postgres-postgresql-ha-postgresql-0 -- /bin/bash
```

### Connect to Postgres
```
	psql --user=postgres --port=5432
	psql -h localhost -U postgres --password -p 5432 
```

### Create Database and Users
```
	CREATE DATABASE db_iceberg;
	CREATE DATABASE db_hue;
	CREATE DATABASE db_superset;
	CREATE DATABASE db_datahub;
	UPDATE pg_database SET datallowconn = 'true' WHERE datname = 'db_iceberg';
	UPDATE pg_database SET datallowconn = 'true' WHERE datname = 'db_hue';
	UPDATE pg_database SET datallowconn = 'true' WHERE datname = 'db_superset';
	UPDATE pg_database SET datallowconn = 'true' WHERE datname = 'db_datahub';

	CREATE ROLE role_iceberg LOGIN PASSWORD 'XXXXXX';
	CREATE ROLE role_hue LOGIN PASSWORD 'XXXXXX';
	CREATE ROLE role_superset LOGIN PASSWORD 'XXXXX';
	CREATE ROLE role_datahub LOGIN PASSWORD 'XXXXX';

	GRANT CONNECT ON DATABASE db_iceberg TO role_iceberg;
	GRANT CONNECT ON DATABASE db_hue TO role_hue;
	GRANT CONNECT ON DATABASE db_datahub TO role_datahub;
	GRANT CONNECT ON DATABASE db_superset TO role_superset;


	GRANT ALL privileges ON SCHEMA public TO role_hue;
	GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO role_hue;

	GRANT ALL privileges ON SCHEMA public TO role_iceberg;
	GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO role_iceberg;

	GRANT ALL privileges ON SCHEMA public TO role_superset;
	GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO role_superset;

	GRANT ALL privileges ON SCHEMA public TO role_datahub;
	GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO role_datahub;
```

# Install Spark

### Create the namespace
```
	kc delete namespace rebios-spark
	kc create namespace rebios-spark
```

### Create master volume
file: spark-pv.yaml

```
	apiVersion: v1
	kind: PersistentVolume
	metadata:
	  name: spark-volume
	  labels:
		type: local
		app: rebios-spark
	spec:
	  storageClassName: manual
	  capacity:
		storage: 5Gi
	  accessModes:
		- ReadWriteMany
	  hostPath:
		path: /data/spark
```

### Apply master volume
```
	kc apply -n rebios-spark -f spark-pv.yaml
	kc get pv
```

### Create master volume claim
file: spark-pvc.yaml

```
	apiVersion: v1
	kind: PersistentVolumeClaim
	metadata:
	  name: spark-volume-claim
	  labels:
		app: rebios-spark
	spec:
	  storageClassName: manual
	  volumeName: spark-volume
	  accessModes:
		- ReadWriteMany
	  resources:
		requests:
		  storage: 5Gi
```

### Apply master volume claim
```
	kc apply -n rebios-spark -f spark-pvc.yaml
	kc -n rebios-spark get pvc
```

### Create the master configmap
file: spark-master-configmap.yaml

```
    apiVersion: v1
	kind: ConfigMap
	metadata:
	  name: rebios-spark-master-secret
	  labels:
		app: rebios-spark
	data:
	  SPARK_WORKLOAD: "master"
	  SPARK_MASTER_PORT: ""
	  DEPENDENCIES: "org.postgresql:postgresql:42.6.0\
					,org.apache.iceberg:iceberg-bundled-guava:1.6.0\
					,org.apache.iceberg:iceberg-core:1.6.0\
					,org.apache.iceberg:iceberg-aws:1.6.0\
					,org.apache.iceberg:iceberg-aws-bundle:1.6.0\
					,org.apache.iceberg:iceberg-spark:1.6.0\
					,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0\
					,org.apache.iceberg:iceberg-spark-extensions-3.5_2.12:1.6.0\
					,org.apache.iceberg:iceberg-hive-runtime:1.6.0\
					,org.apache.iceberg:iceberg-hive-metastore:1.6.0\
					,software.amazon.awssdk:sdk-core:2.20.120\
					,org.slf4j:slf4j-simple:2.0.16"
	  S3_URI: https://s3.amazonaws.com
	  S3_BUCKET: rebios-kube-env/
	  AWS_ACCESS_KEY_ID: XXXXX
	  AWS_SECRET_ACCESS_KEY: XXXXX
	  AWS_REGION: us-east-1
	  POSTGRES_CONNECTION_STRING: jdbc:postgresql://34.228.176.207:32038/db_iceberg
	  POSTGRES_USER: role_iceberg
	  POSTGRES_PASSWORD: XXXXX
	  S3_FULL_URL: s3a://rebios-kube-env/rebios
	  BIOS_CATALOG: bios
	  DEPENDENCIES: org.postgresql:postgresql:42.6.0,org.apache.iceberg:iceberg-bundled-guava:1.6.0,org.apache.iceberg:iceberg-core:1.6.0,org.apache.iceberg:iceberg-aws:1.6.0,org.apache.iceberg:iceberg-spark:1.6.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.iceberg:iceberg-spark-extensions-3.5_2.12:1.6.0,org.apache.iceberg:iceberg-hive-runtime:1.6.0,org.apache.iceberg:iceberg-hive-metastore:1.6.0,org.slf4j:slf4j-simple:2.0.7,com.github.ben-manes.caffeine:caffeine:3.1.8
	  AWS_SDK_VERSION: "2.20.120"
	  AWS_MAVEN_GROUP: software.amazon.awssdk
	  IP: "127.0.0.1"
	  PORT: "10000"

```

### Apply master configmap
```
	kc apply -n rebios-spark -f spark-master-configmap.yaml
	kc -n rebios-spark get configmap
```

### Create the master deployment
file: spark-master-deployment.yaml

```
	kind: Deployment
	apiVersion: apps/v1
	metadata:
	  name: spark-master
	  namespace: rebios-spark
	spec:
	  replicas: 1
	  selector:
		matchLabels:
		  component: spark-master
	  template:
		metadata:
		  labels:
			component: spark-master
		spec:
		  containers:
			- name: spark-master
			  image: tabulario/spark-iceberg
			  envFrom:
				- configMapRef:
					name: rebios-spark-master-secret
			  resources:
				requests:
				  memory: "6Gi"
				limits:
				  memory: "8Gi"
			  command:
			  - /bin/sh
			  - -c
			  - |
				tail -f /dev/null
			  ports:
				- containerPort: 7077
				- containerPort: 8080
			  volumeMounts:
				- mountPath: /tmp
				  name: sparkdata
		  volumes:
			- name: sparkdata
			  persistentVolumeClaim:
				claimName: spark-volume-claim
```

### Apply spark master deployment
```
	kc apply -n rebios-spark -f spark-master-deployment.yaml
	kc -n rebios-spark get deployments
	kc -n rebios-spark get pods
```

### Create the master service
file: spark-master-service.yaml

```
	kind: Service
	apiVersion: v1
	metadata:
	  name: spark-master
	  namespace: rebios-spark
	spec:
	  type: NodePort
	  ports:
		- name: webui
		  port: 7890
		  targetPort: 8080
		  nodePort: 30008
		- name: spark
		  port: 7077
		  targetPort: 7077
		  nodePort: 30007
		- name: hive
		  port: 10000
		  targetPort: 10000
		  nodePort: 30010
	  selector:
		component: spark-master
```

### Apply spark master service
```
	kc create -n rebios-spark -f spark-master-service.yaml 
    kc -n rebios-spark get services
```


### Create the worker configmap
file: spark-worker-configmap.yaml

```
    apiVersion: v1
	kind: ConfigMap
	metadata:
	  name: rebios-spark-master-secret
	  labels:
		app: rebios-spark
	data:
	  SPARK_WORKLOAD: "worker"
	  SPARK_MASTER_PORT: ""
	  DEPENDENCIES: "org.postgresql:postgresql:42.6.0\
					,org.apache.iceberg:iceberg-bundled-guava:1.6.0\
					,org.apache.iceberg:iceberg-core:1.6.0\
					,org.apache.iceberg:iceberg-aws:1.6.0\
					,org.apache.iceberg:iceberg-aws-bundle:1.6.0\
					,org.apache.iceberg:iceberg-spark:1.6.0\
					,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0\
					,org.apache.iceberg:iceberg-spark-extensions-3.5_2.12:1.6.0\
					,org.apache.iceberg:iceberg-hive-runtime:1.6.0\
					,org.apache.iceberg:iceberg-hive-metastore:1.6.0\
					,software.amazon.awssdk:sdk-core:2.20.120\
					,org.slf4j:slf4j-simple:2.0.16"
	  S3_URI: https://s3.amazonaws.com
	  S3_BUCKET: rebios-kube-env/
	  AWS_ACCESS_KEY_ID: XXXXX
	  AWS_SECRET_ACCESS_KEY: XXXXX
	  AWS_REGION: us-east-1
	  POSTGRES_CONNECTION_STRING: jdbc:postgresql://34.228.176.207:32038/db_iceberg
	  POSTGRES_USER: role_iceberg
	  POSTGRES_PASSWORD: XXXXX
	  S3_FULL_URL: s3a://rebios-kube-env/rebios
	  BIOS_CATALOG: bios
	  DEPENDENCIES: org.postgresql:postgresql:42.6.0,org.apache.iceberg:iceberg-bundled-guava:1.6.0,org.apache.iceberg:iceberg-core:1.6.0,org.apache.iceberg:iceberg-aws:1.6.0,org.apache.iceberg:iceberg-spark:1.6.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.iceberg:iceberg-spark-extensions-3.5_2.12:1.6.0,org.apache.iceberg:iceberg-hive-runtime:1.6.0,org.apache.iceberg:iceberg-hive-metastore:1.6.0,org.slf4j:slf4j-simple:2.0.7,com.github.ben-manes.caffeine:caffeine:3.1.8
	  AWS_SDK_VERSION: "2.20.120"
	  AWS_MAVEN_GROUP: software.amazon.awssdk
	  IP: "127.0.0.1"
	  PORT: "10000"
```

### Apply worker configmap
```
	kc apply -n rebios-spark -f spark-worker-configmap.yaml
	kc -n rebios-spark get configmap
```

### Create the worker deployment
file: spark-worker-deployment.yaml

```
	kind: Deployment
	apiVersion: apps/v1
	metadata:
	  name: spark-worker
	  namespace: rebios-spark
	spec:
	  replicas: 2
	  selector:
		matchLabels:
		  component: spark-worker
	  template:
		metadata:
		  labels:
			component: spark-worker
		spec:
		  containers:
			- name: spark-worker
			  image: tabulario/spark-iceberg
			  envFrom:
				- configMapRef:
					name: rebios-spark-worker-secret
			  resources:
				requests:
				  memory: "3Gi"
				limits:
				  memory: "4Gi"
			  command:
			  - /bin/sh
			  - -c
			  - |
				tail -f /dev/null
			  ports:
				- containerPort: 7077
				- containerPort: 8080
			  volumeMounts:
				- mountPath: /tmp
				  name: sparkdata
		  volumes:
			- name: sparkdata
			  persistentVolumeClaim:
				claimName: spark-volume-claim
```

### Apply spark worker deployment
```
	kc apply -n rebios-spark -f spark-worker-deployment.yaml
	kc -n rebios-spark get deployments
	kc -n rebios-spark get pods
```

### Create the worker service
file: spark-worker-service.yaml

```
	kind: Service
	apiVersion: v1
	metadata:
	  name: spark-worker
	  namespace: rebios-spark
	spec:
	  type: NodePort
	  ports:
		- name: webui
		  port: 7890
		  targetPort: 8080
		  nodePort: 30011
		- name: spark
		  port: 7077
		  targetPort: 7077
		  nodePort: 30012
		- name: hive
		  port: 10000
		  targetPort: 10000
		  nodePort: 30013
	  selector:
		component: spark-worker
```


