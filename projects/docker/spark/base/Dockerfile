# ======================
#
# Stage 1a. Download Hadoop
#
# ======================

FROM ubuntu:16.04 AS hadoopBuilder

LABEL maintainer="Sergei Papulin <papulin_bmstu@mail.ru>"

RUN apt-get update \
    && apt-get install -y wget \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Hadoop
RUN mkdir /download \
    && wget -P /download https://archive.apache.org/dist/hadoop/common/hadoop-3.1.2/hadoop-3.1.2.tar.gz \
    && tar -xvf /download/hadoop-3.1.2.tar.gz --directory /download --strip-components 1 \
    && rm /download/hadoop-3.1.2.tar.gz


# ======================
#
# Stage 1b. Download Spark
#
# ======================

FROM ubuntu:16.04 AS sparkBuilder

LABEL maintainer="Sergei Papulin <papulin_bmstu@mail.ru>"

RUN apt-get update \
    && apt-get install -y wget \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

#Install Spark
RUN mkdir /download \
    && wget -P /download https://archive.apache.org/dist/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz \
    && tar -xvf /download/spark-2.4.7-bin-hadoop2.7.tgz --directory /download --strip-components 1 \
    && rm /download/spark-2.4.7-bin-hadoop2.7.tgz

# Note: It will download the archive each time when you build an image
# ADD https://archive.apache.org/dist/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz /download/

# ======================
#
# Stage 1c. Download Kafka
#
# ======================
 
FROM ubuntu:16.04 AS kafkaBuilder
 
LABEL maintainer="Sergei Papulin <papulin_bmstu@mail.ru>"
 
RUN apt-get update \
    && apt-get install -y wget \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
 
# Install Hadoop
RUN mkdir /download \
    && wget -P /download/ https://apache-mirror.rbc.ru/pub/apache/kafka/2.8.1/kafka_2.12-2.8.1.tgz \
    && tar -xvzf /download/kafka_2.12-2.8.1.tgz --directory /download --strip-components 1 \
    && rm /download/kafka_2.12-2.8.1.tgz

# ======================
#
# Stage 2. Setup Base Image
#
# ======================

FROM ubuntu:18.04 AS main

LABEL maintainer="Sergei Papulin <papulin_bmstu@mail.ru>"

# User home directory
ARG BASE_USER_DIR=/home/bigdata

# Create user
RUN useradd -m -d $BASE_USER_DIR -s /bin/bash bigdata 

COPY --from=hadoopBuilder --chown=bigdata:bigdata /download/ $BASE_USER_DIR/hadoop/
COPY --from=sparkBuilder --chown=bigdata:bigdata /download/ $BASE_USER_DIR/spark/
COPY --from=kafkaBuilder --chown=bigdata:bigdata /download/ $BASE_USER_DIR/kafka/

# Note: ENV doesn't allow updating variables sequentially
ENV \
    # Set Hadoop environment variables
    HADOOP_HOME=$BASE_USER_DIR/hadoop \
    HADOOP_CONF_DIR=$BASE_USER_DIR/hadoop/etc/hadoop \
    # Set Spark environment variables
    SPARK_HOME=$BASE_USER_DIR/spark \
    SPARK_CONF_DIR=$BASE_USER_DIR/spark/conf \
    # Set Kafka environment variables
    KAFKA_HOME=$BASE_USER_DIR/kafka \
    # Add to PATH
    PATH=$BASE_USER_DIR/kafka/bin:$BASE_USER_DIR/spark/bin:$BASE_USER_DIR/spark:$BASE_USER_DIR/hadoop/bin:$BASE_USER_DIR/hadoop:$PATH

# ======================
#
# Install Packages
#
# ======================

RUN \
    # Install system packages
    apt-get update && apt-get install -y \
        locales \
        openssh-server \
        software-properties-common \
        sudo \
        sed \
        nano \
        tree \
        python3-pip \
        net-tools \
        curl \
        htop \
    # Install JDK 8
    && add-apt-repository ppa:openjdk-r/ppa \
    && apt-get -y install openjdk-8-jdk \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    # Upgrade pip
    && python3 -m pip install --no-cache-dir --upgrade pip

COPY --chown=bigdata:bigdata "etc/requirements.txt" "$BASE_USER_DIR/requirements.txt"

RUN \
    # Install python packages from the requirements file
    python3 -m pip install --no-cache-dir -r $BASE_USER_DIR/requirements.txt \
    # Add sudo permission for the bigdata user to anything
    && echo "bigdata ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
    # Set a password to the user
    && usermod --password "$(openssl passwd -1 12345)" bigdata

# ======================
#
# Custom configuration
#
# ======================

# Set the locale (due to twitter messages can not be printed in console of Jupyter container)
RUN locale-gen --no-purge en_US.UTF-8
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG=en_US.UTF-8  
ENV LANGUAGE=en_US:en  
ENV LC_ALL=en_US.UTF-8  

# Copy configuration files
COPY --chown=bigdata:bigdata ["config/hdfs", "config/yarn", "config/mapreduce", "$HADOOP_CONF_DIR/"]
COPY --chown=bigdata:bigdata ["config/spark", "$SPARK_CONF_DIR/"]

# Copy the entrypoint script
COPY --chown=bigdata:bigdata scripts/entrypoint.sh /usr/local/bin/
RUN chmod 500 /usr/local/bin/entrypoint.sh

# Change root to the bigdata user
USER bigdata

# Set current dir
WORKDIR $BASE_USER_DIR

# Create a directory for Spark logs and SSH keys
RUN mkdir -p tmp/spark-events .ssh 

# Expose ports for a master node
EXPOSE 9870 8088 18080 9999 7071 7070

# Default command on start
ENTRYPOINT ["/bin/bash", "/usr/local/bin/entrypoint.sh"]
