cd ..
sudo rm -rf ./iceberg
sudo rm -rf ./spark
cd ./environment

sudo docker rm --force $(sudo docker ps -aq)
sudo docker system prune -a
