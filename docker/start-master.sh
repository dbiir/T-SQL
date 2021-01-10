sudo service ssh start
sudo chmod 777 /etc/hosts

# mkdir /home/gpadmin/gtmp

while [ ! -f "/tmp/iplist.sh" ]
do
    sleep 1
done

source /tmp/iplist.sh

ssh-keyscan -t rsa $SELF_GENERATE_MASTER_HOSTNAME > /home/gpadmin/.ssh/known_hosts

touch /home/gpadmin/conf/seg_hosts

index_of_segment=1
env_of_segment_ip="SELF_GENERATE_SEGMENT_IP_"$index_of_segment
env_of_segment_hostname="SELF_GENERATE_SEGMENT_HOSTNAME_"$index_of_segment
eval "env_of_segment_ip=\$$env_of_segment_ip"
eval "env_of_segment_hostname=\$$env_of_segment_hostname"
while [[ ! -z $env_of_segment_hostname ]]
do
    echo "$env_of_segment_ip $env_of_segment_hostname" >> /etc/hosts
    ssh-keyscan -t rsa $env_of_segment_hostname >> /home/gpadmin/.ssh/known_hosts
    echo $env_of_segment_hostname >> /home/gpadmin/conf/seg_hosts

    let "index_of_segment=$index_of_segment+1"
    env_of_segment_ip="SELF_GENERATE_SEGMENT_IP_"$index_of_segment
    env_of_segment_hostname="SELF_GENERATE_SEGMENT_HOSTNAME_"$index_of_segment
    eval "env_of_segment_ip=\$$env_of_segment_ip"
    eval "env_of_segment_hostname=\$$env_of_segment_hostname"
done

source /home/gpadmin/gp/greenplum_path.sh

export GPPORT=5432
export PGPORT=5432
export GPDATABASE=penguindb
export PGDATABASE=penguindb
export MASTER_DATA_DIRECTORY=/home/gpadmin/gpdata/gpmaster/gpseg-1

sed -i "s/SELF_GENERATE_MASTER_HOSTNAME/$SELF_GENERATE_MASTER_HOSTNAME/g" /home/gpadmin/conf/gpinitsystem_config

# sed -i "s/for seg in gp-seg1 gp-seg2 gp-seg3/for seg in $SELF_GENERATE_SEGMENT_HOSTNAME_1 $SELF_GENERATE_SEGMENT_HOSTNAME_2 $SELF_GENERATE_SEGMENT_HOSTNAME_3/g" /home/gpadmin/gpdb/gpdistribuild.sh

gpinitsystem -a -c /home/gpadmin/conf/gpinitsystem_config

gpconfig -c gp_enable_global_deadlock_detector -v on
gpconfig -c log_statement -v none
gpconfig -c max_connections -v 6000 -m 2000
gpconfig -c max_prepared_transactions -v 2000
gpconfig -c checkpoint_segments -v 2 --skipvalidation
gpconfig -c work_mem -v 1GB
gpconfig -c shared_buffers -v 5GB
gpconfig -c effective_cache_size -v 32GB
gpconfig -c maintenance_work_mem -v 512MB

echo "host     all         gpadmin         0.0.0.0/0 trust" >> /home/gpadmin/gpdata/gpmaster/gpseg-1/pg_hba.conf

gpstop -a
gpstart -a

# psql -d penguindb -c "alter database penguindb set default_transaction_isolation='repeatable read'"
# psql -d penguindb -c "alter database penguindb set transam_mode=occ;"

pgbench -i -s 10 -x storage_engine=rocksdb -n

# index_of_standby=1
# env_of_standby_ip="SELF_GENERATE_STANDBY_IP_"$index_of_standby
# env_of_standby_hostname="SELF_GENERATE_STANDBY_HOSTNAME_"$index_of_standby
# eval "env_of_standby_ip=\$$env_of_standby_ip"
# eval "env_of_standby_hostname=\$$env_of_standby_hostname"
# while [[ ! -z $env_of_standby_hostname ]]
# do
#     echo "$env_of_standby_ip $env_of_standby_hostname" >> /etc/hosts
#     ssh-keyscan -t rsa $env_of_standby_hostname >> /home/gpadmin/.ssh/known_hosts
#     scp /etc/hosts $env_of_standby_hostname:/home/gpadmin/hosts.back
#     ssh $env_of_standby_hostname "sudo chmod 777 /etc/hosts; cat /home/gpadmin/hosts.back >> /etc/hosts"
#     ssh $env_of_standby_hostname "echo \"$SELF_GENERATE_MASTER_IP $SELF_GENERATE_MASTER_HOSTNAME\" >> /etc/hosts"
#     gpinitstandby -a -s $env_of_standby_hostname
#     ssh $env_of_standby_hostname ". /home/gpadmin/gp/greenplum_path.sh; export GPPORT=5432; export PGPORT=5432; export GPDATABASE=penguindb; export PGDATABASE=penguindb; export MASTER_DATA_DIRECTORY=/home/gpadmin/gpdata/gpmaster/gpseg-1; gpactivatestandby -a -f"
#     psql -d template1 -c "set gp_role=utility; select catalog_update_standby_info();"

#     let "index_of_standby=$index_of_standby+1"
#     env_of_standby_ip="SELF_GENERATE_STANDBY_IP_"$index_of_standby
#     env_of_standby_hostname="SELF_GENERATE_STANDBY_HOSTNAME_"$index_of_standby
#     eval "env_of_standby_ip=\$$env_of_standby_ip"
#     eval "env_of_standby_hostname=\$$env_of_standby_hostname"
# done

echo "MASTERINITDONE"

tail -f /dev/null

