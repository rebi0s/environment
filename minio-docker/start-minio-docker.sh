echo 'Usar esse comando somente para testes locais sem ter que instalar na máquina !!'

mkdir -p ~/minio/data

docker run \
   -p 9000:9000 \
   -p 9190:9190 \
   --name minio \
   -v ~/minio/data:/data \
   -e "MINIO_ROOT_USER=admin" \
   -e "MINIO_ROOT_PASSWORD=Eqcu3%#Gq6NV" \
   quay.io/minio/minio server /data --console-address ":9190"
