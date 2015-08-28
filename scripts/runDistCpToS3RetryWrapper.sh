#!/bin/bash

# puts a simple wrapper shell script around hadoop distcp to retry in
# case of certain exit codes. This is intended to copy from hdfs to
# S3.

# soam@altiscale.com
# Altiscale Inc.

# list of bad return codes: -1, -2, -3
BAD_RETURNS=( 253 254 255 )

# make sure we have 3 input arguments
EXPECTED_ARGS=3
E_BADARGS=65

if [ $# -ne $EXPECTED_ARGS ]; then
  echo "Usage: `basename $0` <HDFS src dir> <S3 dest bucket> <S3 dest dir>"
  echo ""
  echo "Copies a local HDFS directory/file to S3 in a more fault tolerant,"
  echo "repeatable manner."
  echo ""
  echo "Assumes AWS access and secret keys are defined in the shell environment"
  echo "variables AWS_ACCESS_KEY and AWS_SECRET_KEY respectively."
  echo ""
  echo "Example: "
  echo "`basename $0` /local/hdfs/path/to/dir mybucket /foo"
  exit $E_BADARGS
fi

NUM_RETRIES=8

# limit the number of copies running at the same time
MAX_PARALLEL_COPIES=5

# input directory name. No trailing slash eg. /a/b
HDFS_INPUT_DIR=$1

# location of destination bucket eg. bucket_foo
S3_DEST_BUCKET=$2

# location where the data will be placed in S3. eg. /a/b
S3_DEST_DIR=$3


# assumes the AWS access and secret keys are defined in the shell environment
# Edit if you wish to embed them here instead
MY_AWS_ACCESS_KEY=${AWS_ACCESS_KEY}
MY_AWS_SECRET_KEY=${AWS_SECRET_KEY}

S3_URI=s3n://$MY_AWS_ACCESS_KEY:$MY_AWS_SECRET_KEY@$S3_DEST_BUCKET$S3_DEST_DIR

if [ -z "$MY_AWS_ACCESS_KEY" ] || [ -z "$MY_AWS_SECRET_KEY" ]; then
  echo "`basename $0`: Missing AWS keys! Either edit script to include or set environment variables AWS_ACCESS_KEY and AWS_SECRET_KEY"
  echo "`basename $0`: Proceeding with the assumption that the AWS keys are added in a hadoop conf file such as core-site.xml"
  S3_URI=s3n://$S3_DEST_BUCKET$S3_DEST_DIR
  # exit $E_BADARGS  
fi



HADOOP=hadoop
TASK_TIMEOUT=1800000
TASK_ATTEMPTS=12

DEFINES="-Dmapred.task.timeout=$TASK_TIMEOUT -Dmapreduce.map.maxattempts=$TASK_ATTEMPTS"

# retry loop
COUNTER=0
while [ $COUNTER -lt $NUM_RETRIES ]; do

  # run the hadoop command
  echo "$HADOOP distcp $DEFINES -i -update -m $MAX_PARALLEL_COPIES $HDFS_INPUT_DIR $S3_URI"
  $HADOOP distcp $DEFINES -i -update -m $MAX_PARALLEL_COPIES $HDFS_INPUT_DIR $S3_URI

  # check the exit value. Looking at the DistCp source, exit code -1
  # through -3 are bad news but we can try repeating others. DistCp
  # actually returns -999 for other exceptions.
  RETVAL=$?
  echo "Completed with exit code of $RETVAL"

  # check exit codes
  if [ $RETVAL -eq 0 ]; then
  	exit 0
  fi

  for val in "${BAD_RETURNS[@]}"; do
	if [ $RETVAL -eq $val ]; then
	  echo "Cannot retry.  Quitting."
	  exit $RETVAL                            
	fi  
  done

  echo "Retrying ..."
  sleep 2

  let COUNTER=COUNTER+1
done

if [ $COUNTER -eq $NUM_RETRIES ] ; then
  exit $RETVAL
else
  exit 0
fi

