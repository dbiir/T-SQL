#!/bin/bash

set -e

LEADER_IP="10.77.110.145"
NUMBER_OF_SEGMENT=3
NUMBER_OF_STANDBY=2

ERROR(){
    echo -e "\e[101m\e[97m[ERROR]\e[49m\e[39m $@"
}

INFO(){
    echo -e "\e[104m\e[97m[INFO]\e[49m\e[39m $@"
}

WARNING(){
    echo -e "\e[101m\e[97m[WARNING]\e[49m\e[39m $@"
}

EXISTCOMMAND() {
    type $1 2>&1 > /dev/null
}

EXISTFILE() {
    if [ "$(ls -alF | grep $1 | wc -l)" != "1" ]
    then
        ERROR "Please download $1."
        exit 1
    fi
}

SENDTOMASTER() {
    if [ ! -n "$(docker ps -a | grep registry)" ]
    then
        docker run -d -p 5000:5000 registry
    fi
    docker tag penguindb-$1:latest $LEADER_IP:5000/penguindb-$1:latest
    docker push $LEADER_IP:5000/penguindb-$1:latest
    ssh 10.77.110.144 "docker pull $LEADER_IP:5000/penguindb-$1:latest 2>&1 > /dev/null; docker tag $LEADER_IP:5000/penguindb-$1:latest penguindb-$1:latest; docker rmi $LEADER_IP:5000/penguindb-$1:latest; docker rm \$(docker stop \$(docker ps -a | grep Exited)); docker image prune -f"
}

SENDTOFOLLOWER() {
    if [ ! -n "$(docker ps -a | grep registry)" ]
    then
        docker run -d -p 5000:5000 registry
    fi
    docker tag penguindb-$1:latest $LEADER_IP:5000/penguindb-$1:latest
    docker push $LEADER_IP:5000/penguindb-$1:latest
    for follower_ip in 10.77.110.146
    do
        ssh $follower_ip "docker pull $LEADER_IP:5000/penguindb-$1:latest 2>&1 > /dev/null; docker tag $LEADER_IP:5000/penguindb-$1:latest penguindb-$1:latest; docker rmi $LEADER_IP:5000/penguindb-$1:latest; docker rm \$(docker stop \$(docker ps -a | grep Exited)); docker image prune -f" &
    done
    ssh 10.77.110.147 "docker pull $LEADER_IP:5000/penguindb-$1:latest 2>&1 > /dev/null; docker tag $LEADER_IP:5000/penguindb-$1:latest penguindb-$1:latest; docker rmi $LEADER_IP:5000/penguindb-$1:latest; docker rm \$(docker stop \$(docker ps -a | grep Exited)); docker image prune -f"
}

POSITIONAL=()
while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        -s|--stop)
            STOP=1
            shift
            ;;
        -a|--alpha)
            ALPHA=1
            shift
            ;;
        -b|--beta)
            BETA=1
            shift
            ;;
        -c|--compile)
            COMPILE=1
            shift
            ;;
        -d|--delta)
            DELTA=1
            shift
            ;;
        -t|--test)
            TEST=1
            shift
            ;;
        -m|--master)
            NUMBER_OF_STANDBY=$2
            shift
            shift
            ;;
        -v|--segment)
            NUMBER_OF_SEGMENT=$2
            shift
            shift
            ;;
        *)
            POSITIONAL+=("$1")
            ERROR "unknown option: $1"
            shift
            exit 1
            ;;
    esac
done

set -- "${POSITIONAL[@]}"

if [ "$STOP" ]
then
    set +e

    INFO "Stopping penguindb cluster on k8s..."
    kubectl delete deployment.apps/penguindb-runtest
    kubectl delete deployment.apps/penguindb-master
    kubectl delete deployment.apps/penguindb-segment
    kubectl delete deployment.apps/penguindb-standby

    INFO "Waiting for service exit..."
    while [ -n "$(kubectl get pods | grep runtest)" ]
    do
        sleep 1
    done
    while [ -n "$(kubectl get pods | grep master)" ]
    do
        sleep 1
    done
    while [ -n "$(kubectl get pods | grep segment)" ]
    do
        sleep 1
    done
    while [ -n "$(kubectl get pods | grep standby)" ]
    do
        sleep 1
    done

    docker rm $(docker ps -a | grep Exited | awk '{print $1}')
    docker rmi $(docker images | grep none | awk '{print $3}')

    set -e
    INFO "Done."
fi

if [ "$ALPHA" ]
then
    set +e
    docker rm $(docker ps -a | grep Exited | awk '{print $1}')
    docker rmi $(docker images | grep none | awk '{print $3}')
    set -e

    EXISTCOMMAND ssh-keygen || { ERROR "Please install ssh tools."; exit 1; }

    touch id_rsa
    rm -rf ./id_rsa*
    INFO "Generating key pair..."
    ssh-keygen -t rsa -N "" -f ./id_rsa
    INFO "Done."

    EXISTFILE "protobuf-all-3.9.1.tar.gz"

    EXISTCOMMAND docker || { ERROR "Please install docker."; exit 1; }

    INFO "Generating alpha image..."
    docker build -t yusu-alpha:latest -f dockerfile-alpha .
    INFO "Done."
fi

if [ "$BETA" ]
then
    set +e
    docker rm $(docker ps -a | grep Exited | awk '{print $1}')
    docker rmi $(docker images | grep none | awk '{print $3}')
    set -e

    touch gpdb
    rm -rf gpdb
    mkdir gpdb
    INFO "Copying source files from current directory..."
    (cd ..; tar --exclude=./docker --exclude=./.git -cf - .) | tar Cxf ./gpdb -
    INFO "Done."

    INFO "Generating LTS images..."
    docker build -t penguindb-lts:latest -f dockerfile-lts .

    SENDTOMASTER lts

    EXISTCOMMAND kubectl || { ERROR "Please install kubernetes."; exit 1; }

    INFO "Starting LTS service..."
    kubectl apply -f deploy-lts.yaml
    INFO "LTS Started."
fi

if [ "$COMPILE" ]
then
    set +e
    docker rm $(docker ps -a | grep Exited | awk '{print $1}')
    docker rmi $(docker images | grep none | awk '{print $3}')
    set -e

    touch gpdb
    rm -rf gpdb
    mkdir gpdb
    INFO "Copying source files from current directory..."
    (cd ..; tar --exclude=./docker --exclude=./.git -cf - .) | tar Cxf ./gpdb -
    INFO "Done."

    INFO "Generating beta image..."
    docker build -t yusu-beta:latest -f dockerfile-beta .
    INFO "Done."
fi

if [ "$DELTA" ]
then
    set +e
    docker rm $(docker ps -a | grep Exited | awk '{print $1}')
    docker rmi $(docker images | grep none | awk '{print $3}')
    set -e

    touch gpdb
    rm -rf gpdb
    mkdir gpdb
    INFO "Copying source files from current directory..."
    (cd ..; tar --exclude=./docker --exclude=./.git -cf - .) | tar Cxf ./gpdb -
    kubectl get pods | grep lts | awk '{print $1}'| xargs -i kubectl exec {} ifconfig | grep inet | grep -v 127.0.0.1 | awk '{print $2}' > iplist.sh
    sed -i "s/9.39.242.133/$(cat iplist.sh)/g" gpdb/src/backend/access/kv/timestamp_transaction/http.c
    sed -i "s/9.39.242.133/$(cat iplist.sh)/g" gpdb/src/backend/libtcp/libtcpforcpp.cpp
    rm iplist.sh
    INFO "Done."

    EXISTFILE dstat

    INFO "Generating compile image..."
    docker build -t yusu-compile:latest -f dockerfile-compile .
    INFO "Done."

    INFO "Generating all penguindb images..."
    docker build -t penguindb-master:latest -f dockerfile-master .
    docker build -t penguindb-segment:latest -f dockerfile-segment .
    docker build -t penguindb-standby:latest -f dockerfile-standby .
    # docker build -t penguindb-pgpool:latest -f dockerfile-pgpool .
    docker build -t penguindb-runtest:latest -f dockerfile-runtest .
    INFO "Done."

    INFO "Distributing images to follower nodes..."
    SENDTOMASTER master
    SENDTOFOLLOWER segment
    SENDTOFOLLOWER standby
    # SENDTOMASTER pgpool
    SENDTOMASTER runtest
    SENDTOFOLLOWER runtest
    INFO "Done."
fi

if [ "$TEST" ]
then
    set +e
    docker rm $(docker ps -a | grep Exited | awk '{print $1}')
    docker rmi $(docker images | grep none | awk '{print $3}')
    set -e

    INFO "Starting penguindb cluster..."
    kubectl apply -f deploy-master.yaml
    sed -i "s/replicas: \([0-9]\+\)/replicas: $NUMBER_OF_SEGMENT/g" deploy-segment.yaml
    kubectl apply -f deploy-segment.yaml
    sed -i "s/replicas: \([0-9]\+\)/replicas: $NUMBER_OF_STANDBY/g" deploy-standby.yaml
    kubectl apply -f deploy-standby.yaml
    kubectl apply -f deploy-runtest.yaml
    sleep 10
    echo "export SELF_GENERATE_MASTER_IP=$(kubectl get pods | grep master | awk '{print $1}'| xargs -i kubectl exec {} ifconfig | grep inet | grep -v 127.0.0.1 | awk '{print $2}')" > iplist.sh
    echo "export SELF_GENERATE_MASTER_HOSTNAME=$(kubectl get pods | grep master | awk '{print $1}'| xargs -i kubectl exec {} cat /etc/hostname)" >> iplist.sh
    kubectl get pods | grep segment | awk '{print $1}'| xargs -i kubectl exec {} ifconfig | grep inet | grep -v 127.0.0.1 | awk '{print $2}' > seg_ip
    index=1
    for ip in $(cat seg_ip)
    do
        echo "export SELF_GENERATE_SEGMENT_IP_$index=$ip" >> iplist.sh
        let "index=$index+1"
    done
    kubectl get pods | grep segment | awk '{print $1}'| xargs -i kubectl exec {} cat /etc/hostname > seg_hostname
    index=1
    for hostname in $(cat seg_hostname)
    do
        echo "export SELF_GENERATE_SEGMENT_HOSTNAME_$index=$hostname" >> iplist.sh
        let "index=$index+1"
    done
    kubectl get pods | grep standby | awk '{print $1}'| xargs -i kubectl exec {} ifconfig | grep inet | grep -v 127.0.0.1 | awk '{print $2}' > seg_ip
    index=1
    for ip in $(cat seg_ip)
    do
        echo "export SELF_GENERATE_STANDBY_IP_$index=$ip" >> iplist.sh
        let "index=$index+1"
    done
    kubectl get pods | grep standby | awk '{print $1}'| xargs -i kubectl exec {} cat /etc/hostname > seg_hostname
    index=1
    for hostname in $(cat seg_hostname)
    do
        echo "export SELF_GENERATE_STANDBY_HOSTNAME_$index=$hostname" >> iplist.sh
        let "index=$index+1"
    done
    kubectl cp ./iplist.sh $(kubectl get pods | grep master | awk '{print $1}'):/tmp/
    while [ ! -n "$(kubectl logs 2>/dev/null $(kubectl get pods | grep master | awk '{print $1}') | grep MASTERINITDONE)" ]
    do
        sleep 1
    done
    kubectl cp ./iplist.sh $(kubectl get pods | grep runtest | awk '{print $1}'):/tmp/
    while [ ! -n "$(kubectl logs 2>/dev/null $(kubectl get pods | grep runtest | awk '{print $1}') | grep RUNTESTDONE)" ]
    do
        sleep 1
    done
    touch result-seg$NUMBER_OF_SEGMENT-256t
    set +e
    rm -rf result-seg$NUMBER_OF_SEGMENT-256t
    set -e
    mkdir result-seg$NUMBER_OF_SEGMENT-256t
    kubectl cp $(kubectl get pods | grep runtest | awk '{print $1}'):/home/gpadmin/result ./result-seg$NUMBER_OF_SEGMENT-256t
    INFO "Done."
fi

