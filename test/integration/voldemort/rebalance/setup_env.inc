# set up the common variables
# may need to consolidate later

# figure out the basedir and voldemort dir
REBALANCE_TEST_DIR=`dirname $0`
REBALANCE_TEST_DIR=`(cd $REBALANCE_TEST_DIR&& pwd)`
cd $REBALANCE_TEST_DIR
REBALANCE_TEST_DIR=`(cd $REBALANCE_TEST_DIR&& pwd)`

VLDMDIR=`dirname $0`/../../../..
VLDMDIR=`(cd $VLDMDIR && pwd)`
cd $VLDMDIR
VLDMDIR=`(cd $VLDMDIR && pwd)`

cd $REBALANCE_TEST_DIR

TESTCFG_PREFIX=$REBALANCE_TEST_DIR/config/test_config
TESTCFG_DIR=$REBALANCE_TEST_DIR/test_config
ORIGCFG_DIR=$REBALANCE_TEST_DIR/config
TESTCFG_PREFIX=$TESTCFG_DIR/test_config
WORKDIR=$REBALANCE_TEST_DIR
TMPCLUSTER=$WORKDIR/tmpcluster
log_dir=$REBALANCE_TEST_DIR/log
mkdir -p log_dir
vldm_dir=$VLDMDIR
work_dir=$REBALANCE_TEST_DIR

# set up the remote run
# expect remote server to have the same structure
TOTAL_NUM_SERVERS=3
REMOTE_RUN=true
SERVER_MACHINES=(localhost localhost localhost)

