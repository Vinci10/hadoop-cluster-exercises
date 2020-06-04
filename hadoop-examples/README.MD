# Exercises data preparation
Simply run `copy_files_to_docker.sh` (or `copy_files_to_docker.bat` for Windows)
Script runs `docker cp` command to put files directory to docker container.

# Push jar to docker
Simply run `build_app_and_copy_to_docker.sh` (or `build_app_and_copy_to_docker.bat` for Windows)
Script runs `mvn package` to build the application and `docker cp` command to put jar to docker container.