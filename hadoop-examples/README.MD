# Exercises data preparation
Simply run `copy_files_to_docker.sh` (or `copy_files_to_docker.bat` for Windows)
Script runs `docker cp` command to put files directory and hdfs setup script to docker container.

# Push jar to docker
Simply run `build_app_and_copy_to_docker.sh` (or `build_app_and_copy_to_docker.bat` for Windows)
Script runs `mvn package` to build the application and `docker cp` command to put jar to docker container.


# How to run app on docker in hdfs

1. Run script to setup directory structure `sh /files/setup_hdfs.sh` 

2. Run specific class with application jar: 

`hadoop -jar <jar_name> <class_name> <hdfs_input_directory> <hdfs_output_directory>`

ex. `hadoop -jar hadoop.jar hadoop.Exercise1 /user/root/ex1/input/ /user/root/ex1/out`

- jar_name - application jar file
- class_name - class name that contains `main` method
- hdfs_input_directory - directory on hdfs that contains input files
- hdfs_output_directory - directory where output should be redirected (should not exist!)