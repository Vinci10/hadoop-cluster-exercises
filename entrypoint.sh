#!/usr/bin/env bash

function configure_ssh_server() {
    ssh-keygen -q -t rsa -f /etc/ssh/ssh_host_rsa_key -N ''
    chmod 0400 /etc/ssh/ssh_host_rsa_key
    chmod 0644 /etc/ssh/ssh_host_rsa_key.pub

    sed -i 's/^#PubkeyAuthentication yes/PubkeyAuthentication yes/g' /etc/ssh/sshd_config
    sed -i 's/.*AuthorizedKeysFile.*/AuthorizedKeysFile %h\/.ssh\/authorized_keys/g' /etc/ssh/sshd_config
    sed -i 's/PasswordAuthentication yes/PasswordAuthentication no/g' /etc/ssh/sshd_config
    sed -i 's/UsePAM yes/UsePAM no/g' /etc/ssh/sshd_config

    /usr/sbin/sshd
}

function generate_ssh_keys() {
    ssh-keygen -q -t rsa -f /root/.ssh/id_rsa -N ""
    cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
    chmod 0600 /root/.ssh/authorized_keys

    cat <<EOF > /root/.ssh/config
Host *
  StrictHostKeyChecking no
  IdentityFile ~/.ssh/id_rsa
EOF
}

function configure_hadoop() {
  local _master_node_hostname=$1

  for config_file in /tmp/config/*.xml; do
    sed "s/MASTER_NODE_HOSTNAME/$_master_node_hostname/g" "$config_file" > "$HADOOP_CONF_DIR/$(basename "$config_file")"
  done
}

source /etc/profile.d/config.sh
configure_ssh_server

if [[ "${NODE_TYPE:-worker}" == "master" ]]; then
  if [ ! -f "$HADOOP_CONF_DIR/master" ]; then
    generate_ssh_keys
    configure_hadoop "$(hostname)"
    echo "$(hostname)" > "$HADOOP_CONF_DIR/master"
  fi
  hdfs namenode -format
  hdfs --config "$HADOOP_CONF_DIR" namenode |& tee "$HADOOP_HOME/logs/namenode.log" &
  yarn --config "$HADOOP_CONF_DIR" timelineserver |& tee "$HADOOP_HOME/logs/timelineserver.log" &
  yarn --config "$HADOOP_CONF_DIR" resourcemanager |& tee "$HADOOP_HOME/logs/resourcemanager.log"
else
  if ! grep -q "$(hostname)" "$HADOOP_CONF_DIR/workers"; then
    grep -q "localhost" "$HADOOP_CONF_DIR/workers" && rm -f "$HADOOP_CONF_DIR/workers"
    echo "$(hostname)" >> "$HADOOP_CONF_DIR/workers"
  fi
  until [ -f "$HADOOP_CONF_DIR/master" ]; do
      sleep 10
  done
  hdfs --config "$HADOOP_CONF_DIR" datanode |& tee "$HADOOP_HOME/logs/datanode.log" &
  yarn --config "$HADOOP_CONF_DIR" nodemanager |& tee "$HADOOP_HOME/logs/nodemanager.log"
fi

exec "$@"