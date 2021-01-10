#!/usr/bin/python2

import glob
import optparse
import os
import shutil
import stat
import subprocess
import sys

from builds.GpBuild import GpBuild

INSTALL_DIR = "/usr/local/gpdb"
DEPENDENCY_INSTALL_DIR = "/usr/local"
SCRIPT_LOC=os.path.dirname(os.path.realpath(__file__))
GPDB_SRC_DIR="{0}/../../".format(SCRIPT_LOC)


def copy_installed(output_dir):
    if os.path.normpath(INSTALL_DIR) != os.path.normpath(output_dir):
        status = subprocess.call("mkdir -p " + output_dir, shell=True)
        if status:
            return status
        return subprocess.call("cp -r %s/*  %s" % (INSTALL_DIR, output_dir), shell=True)
    return


def print_compiler_version():
    status = subprocess.call(["g++", "--version"])
    if status:
        return status
    return subprocess.call(["gcc", "--version"])


def create_gpadmin_user():
    status = subprocess.call("{0}/concourse/scripts/setup_gpadmin_user.bash".format(GPDB_SRC_DIR))
    os.chmod('/bin/ping', os.stat('/bin/ping').st_mode | stat.S_ISUID)
    if status:
        return status

def extract_explain_test_suite():
    tarfiles = glob.glob('explain_test_suite/*.tar.gz')
    if len(tarfiles) != 1:
        print("Expected to find 1 tar file.")
        return 1
    status = subprocess.call(["tar", "xvf", tarfiles[0]])
    return status

def tar_explain_output():
    status = subprocess.call(["tar", "czvf", "icg_output/explain_ouput.tar.gz", "out/"])
    return status

def copy_output():
    for dirpath, dirs, diff_files in os.walk('gpdb_src/'):
        if 'regression.diffs' in diff_files:
            diff_file = dirpath + '/' + 'regression.diffs'
            print("======================================================================\n" +
                  "DIFF FILE: " + diff_file + "\n" +
                  "----------------------------------------------------------------------")
            with open(diff_file, 'r') as fin:
                print fin.read()
    shutil.copyfile("gpdb_src/src/test/regress/regression.diffs", "icg_output/regression.diffs")
    shutil.copyfile("gpdb_src/src/test/regress/regression.out", "icg_output/regression.out")

def install_dependencies(ci_common, dependencies, install_dir):
    for dependency in dependencies:
        status = ci_common.install_dependency(dependency, install_dir)
        if status:
            return status


def fail_on_error(status):
    if status:
        sys.exit(status)


def main():
    parser = optparse.OptionParser()
    parser.add_option("--build_type", dest="build_type", default="RELEASE")
    parser.add_option("--mode", choices=['orca', 'planner'])
    parser.add_option("--compiler", dest="compiler")
    parser.add_option("--cxxflags", dest="cxxflags")
    parser.add_option("--output_dir", dest="output_dir", default=INSTALL_DIR)
    parser.add_option("--configure-option", dest="configure_option", action="append",
                      help="Configure flags, ex --configure_option=--disable-orca --configure_option=--disable-gpcloud")
    parser.add_option("--gcc-env-file", dest="gcc_env_file", help="GCC env file to be sourced")
    parser.add_option("--orca-in-gpdb-install-location", dest="orca_in_gpdb_install_location", action="store_true",
                      help="Install ORCA header and library files in GPDB install directory")
    parser.add_option("--action", choices=['build', 'test', 'test_explain_suite'], dest="action", default='build',
                      help="Build GPDB or Run Install Check")
    parser.add_option("--gpdb_name", dest="gpdb_name")
    parser.add_option("--dbexists", dest="dbexists", action="store_true", default=False, help="create demo cluster or not")
    (options, args) = parser.parse_args()

    gpBuild = GpBuild(options.mode)
    status = print_compiler_version()
    fail_on_error(status)

    # optional gcc env file to be source before executing configure, make, make install commands
    gpBuild.set_gcc_env_file(options.gcc_env_file)

    install_dir = INSTALL_DIR if options.orca_in_gpdb_install_location else DEPENDENCY_INSTALL_DIR
    if options.action.startswith('test'):
        # if required, install orca and xerces library & header
        # in the install directory of gpdb to avoid packaging from multiple directories
        status = gpBuild.install_dependency(options.gpdb_name, INSTALL_DIR)
        fail_on_error(status)

    # install any dependencies specified on the command line
    status = install_dependencies(gpBuild, args, install_dir)
    fail_on_error(status)

    configure_option = []
    if options.configure_option:
        configure_option.extend(options.configure_option)

    # add DEPENDENCY_INSTALL_LOC and INSTALL_DIR paths to configure options
    configure_option.append(
        '"--with-libs={0} {1}"'.format(os.path.join(DEPENDENCY_INSTALL_DIR, "lib"), os.path.join(INSTALL_DIR, "lib")))
    configure_option.append('"--with-includes={0} {1}"'.format(os.path.join(DEPENDENCY_INSTALL_DIR, "include"),
                                                               os.path.join(INSTALL_DIR, "include")))
    gpBuild.append_configure_options(configure_option)

    status = gpBuild.configure()
    fail_on_error(status)

    # compile and install gpdb
    if options.action == 'build':
        status = gpBuild.make()
        fail_on_error(status)

        status = gpBuild.make_install()
        fail_on_error(status)

        status = gpBuild.unittest()
        fail_on_error(status)

        status = copy_installed(options.output_dir)
        fail_on_error(status)
    # run install-check tests
    elif options.action == 'test':
        status = create_gpadmin_user()
        fail_on_error(status)
        if os.getenv("TEST_SUITE", "icg") == 'icw':
            status = gpBuild.install_check('world')
        else:
            status = gpBuild.install_check()
        if status:
            copy_output()
        return status

    elif options.action == 'test_explain_suite':
        status = create_gpadmin_user()
        fail_on_error(status)
        status = extract_explain_test_suite()
        fail_on_error(status)
        status = gpBuild.run_explain_test_suite(options.dbexists)
        fail_on_error(status)
        status = tar_explain_output()
        fail_on_error(status)
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
