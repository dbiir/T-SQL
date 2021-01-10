#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/common.bash"

function gen_env(){
	cat > /home/gpadmin/run_regression_gpcheckcloud.sh <<-EOF
	set -exo pipefail

	source /opt/gcc_env.sh
	source /usr/local/greenplum-db-devel/greenplum_path.sh

	cd "\${1}/gpdb_src/gpcontrib/gpcloud/regress"
	bash gpcheckcloud_regress.sh
	EOF

	chown gpadmin:gpadmin /home/gpadmin/run_regression_gpcheckcloud.sh
	chmod a+x /home/gpadmin/run_regression_gpcheckcloud.sh
}

function run_regression_gpcheckcloud() {
	su gpadmin -c "bash /home/gpadmin/run_regression_gpcheckcloud.sh $(pwd)"
}

function setup_gpadmin_user() {
	./gpdb_src/concourse/scripts/setup_gpadmin_user.bash "centos"
}

function _main() {
	time install_and_configure_gpdb
	time setup_gpadmin_user
	sed -i s/1024/unlimited/ /etc/security/limits.d/90-nproc.conf
	time gen_env

	time run_regression_gpcheckcloud
}

_main "$@"
