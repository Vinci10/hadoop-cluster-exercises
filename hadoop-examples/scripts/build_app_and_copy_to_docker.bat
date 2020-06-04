cd ..
mvn package && docker cp target\hadoop.jar hadoop-cluster-exercises_node-master_1:/files/