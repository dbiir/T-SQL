cd /root/LTS
sed -i "s/SELF_GENERATE_LTS_IP/$(ifconfig | grep inet | grep -v 127.0.0.1 | awk '{print $2}')/g" conf/config.toml
sbin/start-lts-cluster.sh

echo 'LTSINITDONE'

tail -f /dev/null
