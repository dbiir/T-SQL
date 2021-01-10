while [ ! -f "/tmp/iplist.sh" ]
do
    sleep 1
done

source /tmp/iplist.sh

source /home/gpadmin/gp/greenplum_path.sh
export GPPORT=5432
export PGPORT=5432
export GPDATABASE=penguindb
export PGDATABASE=penguindb
export MASTER_DATA_DIRECTORY=/home/gpadmin/gpdata/gpmaster/gpseg-1

ssh-keyscan -t rsa $SELF_GENERATE_MASTER_IP > /home/gpadmin/.ssh/known_hosts
index_of_segment=1
env_of_segment_ip="SELF_GENERATE_SEGMENT_IP_"$index_of_segment
eval "env_of_segment_ip=\$$env_of_segment_ip"
while [[ ! -z $env_of_segment_ip ]]
do
    ssh-keyscan -t rsa $env_of_segment_ip >> /home/gpadmin/.ssh/known_hosts

    let "index_of_segment=$index_of_segment+1"
    env_of_segment_ip="SELF_GENERATE_SEGMENT_IP_"$index_of_segment
    eval "env_of_segment_ip=\$$env_of_segment_ip"
done

# index_of_standby=1
# env_of_standby_ip="SELF_GENERATE_STANDBY_IP_"$index_of_standby
# env_of_standby_hostname="SELF_GENERATE_STANDBY_HOSTNAME_"$index_of_standby
# eval "env_of_standby_ip=\$$env_of_standby_ip"
# eval "env_of_standby_hostname=\$$env_of_standby_hostname"
# while [[ ! -z $env_of_standby_hostname ]]
# do
#     echo "$env_of_standby_ip $env_of_standby_hostname" >> /etc/hosts
#     ssh-keyscan -t rsa $env_of_standby_hostname >> /home/gpadmin/.ssh/known_hosts

#     let "index_of_standby=$index_of_standby+1"
#     env_of_standby_ip="SELF_GENERATE_STANDBY_IP_"$index_of_standby
#     env_of_standby_hostname="SELF_GENERATE_STANDBY_HOSTNAME_"$index_of_standby
#     eval "env_of_standby_ip=\$$env_of_standby_ip"
#     eval "env_of_standby_hostname=\$$env_of_standby_hostname"
# done

# pgbench -i -s 10 -x storage_engine=rocksdb -n -h $SELF_GENERATE_MASTER_IP -p 5432 -U gpadmin

mkdir /home/gpadmin/result

number_of_threads=(10 30 50 70 90 100 110 130 150 170 190 200)
for var in ${number_of_threads[@]}
do
    dstat --output /home/gpadmin/so-$var-onpgbench.csv 2>&1 >/dev/null &
    ssh $SELF_GENERATE_MASTER_IP "nohup dstat --output /home/gpadmin/so-$var-onmaster.csv 2>&1 >/dev/null" &
    index_of_segment=1
    env_of_segment_ip="SELF_GENERATE_SEGMENT_IP_"$index_of_segment
    eval "env_of_segment_ip=\$$env_of_segment_ip"
    while [[ ! -z $env_of_segment_ip ]]
    do
        ssh $env_of_segment_ip "nohup dstat --output /home/gpadmin/so-$var-onsegment$index_of_segment.csv 2>&1 >/dev/null" &

        let "index_of_segment=$index_of_segment+1"
        env_of_segment_ip="SELF_GENERATE_SEGMENT_IP_"$index_of_segment
        eval "env_of_segment_ip=\$$env_of_segment_ip"
    done
    # index_of_standby=1
    # env_of_standby_ip="SELF_GENERATE_STANDBY_IP_"$index_of_standby
    # eval "env_of_standby_ip=\$$env_of_standby_ip"
    # while [[ ! -z $env_of_standby_ip ]]
    # do
    #     ssh $env_of_standby_ip "nohup dstat --output /home/gpadmin/so-$var-onstandby$index_of_standby.csv 2>&1 >/dev/null" &

    #     let "index_of_standby=$index_of_standby+1"
    #     env_of_standby_ip="SELF_GENERATE_STANDBY_IP_"$index_of_standby
    #     eval "env_of_standby_ip=\$$env_of_standby_ip"
    # done

    pgbench -c $var -j $var -r -T 60 -n -S -h $SELF_GENERATE_MASTER_IP -p 5432 -U gpadmin >> /home/gpadmin/so-res.txt

    # index_of_standby=1
    # env_of_standby_ip="SELF_GENERATE_STANDBY_IP_"$index_of_standby
    # eval "env_of_standby_ip=\$$env_of_standby_ip"
    # while [[ ! -z $env_of_standby_ip ]]
    # do
    #     pgbench -c $var -j $var -r -T 60 -n -S -h $env_of_standby_ip -p 5432 -U gpadmin >> /home/gpadmin/so-res-onstandby$index_of_standby.txt &

    #     let "index_of_standby=$index_of_standby+1"
    #     env_of_standby_ip="SELF_GENERATE_STANDBY_IP_"$index_of_standby
    #     eval "env_of_standby_ip=\$$env_of_standby_ip"
    # done

    kill -9 $(ps -aux | grep dstat | grep -v grep | grep -v bash | awk '{print $2}')
    ssh $SELF_GENERATE_MASTER_IP "kill -9 \$(ps -aux | grep dstat | grep -v grep | grep -v bash | awk '{print \$2}')"
    scp $SELF_GENERATE_MASTER_IP:/home/gpadmin/so-$var-onmaster.csv /home/gpadmin

    index_of_segment=1
    env_of_segment_ip="SELF_GENERATE_SEGMENT_IP_"$index_of_segment
    eval "env_of_segment_ip=\$$env_of_segment_ip"
    while [[ ! -z $env_of_segment_ip ]]
    do
        ssh $env_of_segment_ip "kill -9 \$(ps -aux | grep dstat | grep -v grep | grep -v bash | awk '{print \$2}')"
        scp $env_of_segment_ip:/home/gpadmin/so-$var-onsegment$index_of_segment.csv /home/gpadmin

        let "index_of_segment=$index_of_segment+1"
        env_of_segment_ip="SELF_GENERATE_SEGMENT_IP_"$index_of_segment
        eval "env_of_segment_ip=\$$env_of_segment_ip"
    done

    # index_of_standby=1
    # env_of_standby_ip="SELF_GENERATE_STANDBY_IP_"$index_of_standby
    # eval "env_of_standby_ip=\$$env_of_standby_ip"
    # while [[ ! -z $env_of_standby_ip ]]
    # do
    #     ssh $env_of_standby_ip "kill -9 \$(ps -aux | grep dstat | grep -v grep | grep -v bash | awk '{print \$2}')"
    #     scp $env_of_standby_ip:/home/gpadmin/so-$var-onstandby$index_of_standby.csv /home/gpadmin

    #     let "index_of_standby=$index_of_standby+1"
    #     env_of_standby_ip="SELF_GENERATE_STANDBY_IP_"$index_of_standby
    #     eval "env_of_standby_ip=\$$env_of_standby_ip"
    # done
done

mkdir /home/gpadmin/result/so
mv /home/gpadmin/so-res.txt /home/gpadmin/result/so/
mkdir /home/gpadmin/result/so/pgbench
mv /home/gpadmin/*onpgbench.csv /home/gpadmin/result/so/pgbench/
mkdir /home/gpadmin/result/so/master
mv /home/gpadmin/*onmaster.csv /home/gpadmin/result/so/master/

index_of_segment=1
env_of_segment_ip="SELF_GENERATE_SEGMENT_IP_"$index_of_segment
eval "env_of_segment_ip=\$$env_of_segment_ip"
while [[ ! -z $env_of_segment_ip ]]
do
    mkdir /home/gpadmin/result/so/segment$index_of_segment
    mv /home/gpadmin/*onsegment$index_of_segment.csv /home/gpadmin/result/so/segment$index_of_segment/

    let "index_of_segment=$index_of_segment+1"
    env_of_segment_ip="SELF_GENERATE_SEGMENT_IP_"$index_of_segment
    eval "env_of_segment_ip=\$$env_of_segment_ip"
done

# index_of_standby=1
# env_of_standby_ip="SELF_GENERATE_STANDBY_IP_"$index_of_standby
# eval "env_of_standby_ip=\$$env_of_standby_ip"
# while [[ ! -z $env_of_standby_ip ]]
# do
#     mkdir /home/gpadmin/result/so/standby$index_of_standby
#     mv /home/gpadmin/*onstandby$index_of_standby.csv /home/gpadmin/result/so/standby$index_of_standby/

#     let "index_of_standby=$index_of_standby+1"
#     env_of_standby_ip="SELF_GENERATE_STANDBY_IP_"$index_of_standby
#     eval "env_of_standby_ip=\$$env_of_standby_ip"
# done

echo "RUNTESTDONE"

tail -f /dev/null

