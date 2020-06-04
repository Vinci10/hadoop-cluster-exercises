hdfs dfs -mkdir /user/root
hdfs dfs -mkdir /user/root/ex1/input
hdfs dfs -mkdir /user/root/ex2/input
hdfs dfs -mkdir /user/root/ex3/input
hdfs dfs -mkdir /user/root/ex4/input
hdfs dfs -copyFromLocal /files/ex1/ /user/root/ex1/input/
hdfs dfs -copyFromLocal /files/ex2/ /user/root/ex2/input/
hdfs dfs -copyFromLocal /files/ex3/ /user/root/ex3/input/
hdfs dfs -copyFromLocal /files/ex4/ /user/root/ex4/input/