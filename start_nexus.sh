#docker volume create  --name nexus-data
docker run -tid \
   --net=host \
   --privileged=true \
   --restart=always \
   --ulimit nofile=655350 \
   --ulimit memlock=-1 \
   --memory=12G \
   --name nexus3 \
   -e INSTALL4J_ADD_VM_PARAMS="-Xms6000m -Xmx6000m -XX:MaxDirectMemorySize=5000m" \
   -v nexus-data:/nexus-data \
   -v /data/nfsdata:/data/nfsdata \
   -v /etc/resolv.conf:/etc/resolv.conf \
   -v /etc/localtime:/etc/localtime \
  sonatype/nexus3
