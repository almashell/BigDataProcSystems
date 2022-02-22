#!/bin/bash

set -e


main()
{
    NODE_TYPE=$1
    shift

    echo "Command execution..."

    case $NODE_TYPE in
        "master") 
            init_master "$@"
            ;;
        "worker") 
            init_worker "$@"
            ;;
        "jupyter") 
            init_jupyter "$@"
            ;;
        *)
            echo "Error: Unsupported node type."
            exit 127
            ;;
    esac

    echo "The entrypoint script is completed."
    tail -f /dev/null

}


function init_master()
{

    if [ ! -z "$SSH_PRIVATE_KEY" ]; then
        echo "Adding a private key..."
        [ -f .ssh/id_rsa ] && chmod 700 .ssh/id_rsa
        echo $SSH_PRIVATE_KEY >> .ssh/id_rsa
        chmod 500 .ssh/id_rsa
        # TODO: unset SSH_PRIVATE_KEY somehow
    elif [ ! -f .ssh/id_rsa ]; then
        echo "Error: Private key was not found."
        exit 1
    fi

    echo "Starting SSH service..."
    sudo service ssh start

    FILE=/home/bigdata/tmp/hadoop/namenode/current/VERSION
    if [ ! -f $FILE ]; then
        echo "HDFS is not formatted. Formatting..."
        hdfs namenode -format -force
    fi

    echo "Starting Hadoop daemons..."
    hdfs --daemon start namenode
    hdfs --daemon start datanode 
    hdfs --daemon start secondarynamenode
    yarn --daemon start resourcemanager
    yarn --daemon start nodemanager

    echo "Starting Spark History Server..."
    $SPARK_HOME/sbin/start-history-server.sh

    # echo "Starting Jupyter notebook server..."
    # jupyter notebook --config .jupyter/jupyter_notebook_config.py
}


function init_worker()
{

    if [ ! -z "$SSH_PUBLIC_KEY" ]; then
        echo "Adding a public key..."
        echo $SSH_PUBLIC_KEY >> .ssh/id_rsa.pub
        cat .ssh/id_rsa.pub >> .ssh/authorized_keys
        # TODO: unset SSH_PUBLIC_KEY somehow
    elif [ ! -f .ssh/id_rsa.pub ]; then
        echo "Error: Public key was not found."
        exit 1
    fi

    echo "Starting SSH service..."
    sudo service ssh start

    echo "Starting Hadoop daemons..."
    hdfs --daemon start datanode
    yarn --daemon start nodemanager
}

function init_jupyter()
{

    if [ ! -z "$SSH_PUBLIC_KEY" ]; then
        echo "Adding a public key..."
        echo $SSH_PUBLIC_KEY >> .ssh/id_rsa.pub
        cat .ssh/id_rsa.pub >> .ssh/authorized_keys
        # TODO: unset SSH_PUBLIC_KEY somehow
    elif [ ! -f .ssh/id_rsa.pub ]; then
        echo "Error: Public key was not found."
        exit 1
    fi

    echo "Starting SSH service..."
    sudo service ssh start

    echo "Starting Jupyter notebook server..."
    jupyter notebook --config .jupyter/jupyter_notebook_config.py

    # https://theckang.github.io/2015/12/31/remote-spark-jobs-on-yarn.html
    # https://stackoverflow.com/questions/40354624/spark-yarn-cluster-mode-get-this-error-could-not-find-or-load-main-class-org-ap
    hadoop fs -fs "hdfs://master:9000/" -mkdir /user/bigdata/jars
    hdfs dfs -fs "hdfs://master:9000/" -put $SPARK_HOME/jars/* /user/bigdata/jars
}


main "$@"