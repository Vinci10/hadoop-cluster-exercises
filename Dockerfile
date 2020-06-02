FROM centos:latest

ARG JAVA_VERSION=1.8.0

RUN yum install -y "java-$JAVA_VERSION-openjdk-devel" openssh-server openssh-clients tar which && \
    yum clean all && \
    rm -rf /var/cache/yum

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk

ARG HADOOP_VERSION=3.2.1
ARG HADOOP_URL=https://www.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz

WORKDIR /tmp

RUN curl -O https://dist.apache.org/repos/dist/release/hadoop/common/KEYS
RUN gpg --import KEYS

RUN set -eux && \
    curl -fSL "$HADOOP_URL" -o hadoop.tar.gz && \
    curl -fSL "$HADOOP_URL.asc" -o hadoop.tar.gz.asc && \
    gpg --verify hadoop.tar.gz.asc && \
    tar -xvf hadoop.tar.gz -C /opt/ && \
    mv /opt/hadoop-$HADOOP_VERSION /opt/hadoop && \
    rm -f hadoop.tar.gz* KEYS

ENV HADOOP_HOME=/opt/hadoop

WORKDIR $HADOOP_HOME

RUN mkdir logs
RUN mkdir /root/.ssh && \
    chmod 0700 /root/.ssh && \
    echo 'root:root' | chpasswd

RUN echo "export JAVA_HOME=$JAVA_HOME" > /etc/profile.d/config.sh && \
    echo "export HADOOP_HOME=/opt/hadoop" >> /etc/profile.d/config.sh && \
    echo "export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop" >> /etc/profile.d/config.sh && \
    echo "export HADOOP_INSTALL=$HADOOP_HOME" >> /etc/profile.d/config.sh && \
    echo "export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin" >> /etc/profile.d/config.sh && \
    echo "export HDFS_NAMENODE_USER=root" >> /etc/profile.d/config.sh && \
    echo "export HDFS_SECONDARYNAMENODE_USER=root" >> /etc/profile.d/config.sh && \
    echo "export HDFS_DATANODE_USER=root" >> /etc/profile.d/config.sh && \
    echo "export YARN_RESOURCEMANAGER_USER=root" >> /etc/profile.d/config.sh && \
    echo "export YARN_NODEMANAGER_USER=root" >> /etc/profile.d/config.sh

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
COPY config /tmp/config

ENTRYPOINT ["/entrypoint.sh"]