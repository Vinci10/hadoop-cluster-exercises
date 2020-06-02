# Hadoop exercises
Simple configuration to create a hadoop cluster using docker and docker-compose

#### Architecture:

```
Hadoop version: 3.2.1
One master node with services: NameNode, ResourceManager and TimelineServer
Any number of worker nodes with services: DataNode and NodeManager

To add more worker nodes simply add a new service in docker-compose.yml similar to the node-worker-1 service
```

#### Start hadoop cluster:

```
docker-compose build
docker-compose up
```

#### Service URLs:
```
The following service ports will be forwarded to localhost:
 - Namenode: http://localhost:9870
 - TimelineServer: http://localhost:8188
 - Datanode: http://localhost:9864
 - Nodemanager: http://localhost:8042
 - Resourcemanager: http://localhost:8088
```

#### Run a bash shell in master node:

```docker exec -it hadoop-cluster-exercises_node-master_1 bash``` 

#### Close hadoop cluster:

```docker-compose down -v```