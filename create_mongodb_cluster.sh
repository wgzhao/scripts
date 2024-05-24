#!/bin/bash
#--------------------------
# Create mongodb cluster with different cluster mode
# refs:
#   1. https://ankitkumarakt746.medium.com/mongodb-sharded-cluster-with-replica-set-in-docker-81322c903513
#   2. https://www.mongodb.com/resources/products/compatibilities/deploying-a-mongodb-cluster-with-docker
#--------------------------

IMAGE="mongodb/mongodb-community-server:7.0-ubi8"
## also use older image
## #IMAGE="mongo:5"

if [ $# -lt 1 ]; then
  echo "$0 <single|replica|shards> [num]"
  exit 2
fi

cluster_type=$1
num=${2:-3}

docker network rm mongoCluster &> /dev/null
docker network create mongoCluster

if [ "$cluster_type" = "single" ]; then
  docker run -d --rm -p 27017:27017 --name mongo1 --network mongoCluster ${IMAGE} mongod --bind_ip localhost,mongo1
elif [ "$cluster_type" = "replica" ]; then
  port=27017
  for i in $(seq 1 $num); do
    docker run -d --rm -p ${port}:27017 --name mongo$i --network mongoCluster ${IMAGE} mongod --replSet myReplicaSet --bind_ip localhost,mongo${i}
    ((port++))
  done
  # initial the replica set
  #
  sleep 10
  docker exec -it mongo1 mongosh --eval "rs.initiate({
  _id: \"myReplicaSet\",
  members: [
    {_id: 0, host: \"mongo1\"},
    {_id: 1, host: \"mongo2\"},
    {_id: 2, host: \"mongo3\"}
  ]
  })"
elif [ "$cluster_type" = "shards" ]; then
  port=27017
  for i in $(seq 1 $num); do
    docker run -d --rm -p ${port}:27017 --name mongo$i \
      --network mongoCluster ${IMAGE} \
      mongod --shardsvr --replSet myReplicaSet --port 27017 \
      --bind_ip localhost,mongo${i}
    ((port++))
    sleep 1
  done
  sleep 5
  docker exec -it mongo1 mongosh --eval "rs.initiate({
  _id: \"myReplicaSet\",
  members: [
    {_id: 0, host: \"mongo1\"},
    {_id: 1, host: \"mongo2\"},
    {_id: 2, host: \"mongo3\"}
  ]
  })"
fi
exit $?
