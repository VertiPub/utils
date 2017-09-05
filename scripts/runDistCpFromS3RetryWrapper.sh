#!/bin/bash

# puts a simple wrapper shell script around hadoop distcp to retry in
# case of certain exit codes. This is intended to copy from S3 to hdfs.

# soam@altiscale.com
# Altiscale Inc.

# list of bad return codes: -1, -2, -3
BAD_RETURNS=( 253 254 255 )

# make sure we have 3 input arguments
EXPECTED_ARGS=3
E_BADARGS=65

if [ $# -ne $EXPECTED_ARGS ]; then
  echo "Usage: `basename $0` <S3 src bucket> <S3 src dir> <HDFS dest dir>"
  echo ""
  echo "Copies an S3 directory/file to local HDFS in a more fault tolerant,"
  echo "repeatable manner."
  echo ""
  echo "Assumes AWS access and secret keys are defined in the shell environment"
  echo "variables AWS_ACCESS_KEY and AWS_SECRET_KEY respectively."
  echo ""
  echo "Example: "
  echo "`basename $0` mybucket /foo /local/hdfs/path/to/dir"
  exit $E_BADARGS
fi


NUM_RETRIES=8

# assume we're going to be pulling from S3 to VertiCloud cluster
# so can have many mappers. Default is 20 in distcp, but
# starting with 10
MAX_PARALLEL_COPIES=10

# input directory name. No trailing slash eg. /a/b
HDFS_DEST_DIR=$3

# location of destination bucket eg. bucket_foo
S3_SRC_BUCKET=$1

# location where the data will be placed in S3. eg. /a/b
S3_SRC_DIR=$2

# assumes the AWS access and secret keys are defined in the shell environment
# Edit if you wish to embed them here instead
MY_AWS_ACCESS_KEY=${AWS_ACCESS_KEY}
MY_AWS_SECRET_KEY=${AWS_SECRET_KEY}

S3_URI=s3n://$S3_SRC_BUCKET$S3_SRC_DIR

if [ -z "$MY_AWS_ACCESS_KEY" ] || [ -z "$MY_AWS_SECRET_KEY" ]; then
  echo "`basename $0`: Missing AWS keys! Either edit script to include or set environment variables AWS_ACCESS_KEY and AWS_SECRET_KEY"
  echo "`basename $0`: Proceeding with the assumption that the AWS keys are added in a hadoop conf file such as core-site.xml"
  # exit $E_BADARGS  
  S3_URI=s3n://$S3_SRC_BUCKET$S3_SRC_DIR
fi
echo ""


HADOOP=hadoop
TASK_TIMEOUT=1800000
TASK_ATTEMPTS=12

DEFINES="-Dmapred.task.timeout=$TASK_TIMEOUT -Dmapreduce.map.maxattempts=$TASK_ATTEMPTS"

# retry loop
COUNTER=0
while [ $COUNTER -lt $NUM_RETRIES ]; do

  # run the hadoop command
  #echo "$HADOOP distcp $DEFINES -i -update $S3_URI $HDFS_DEST_DIR"
  $HADOOP distcp -Dfs.s3n.awsAccessKeyId=$MY_AWS_ACCESS_KEY -Dfs.s3n.awsSecretAccessKey=$MY_AWS_SECRET_KEY $DEFINES -i -update -m $MAX_PARALLEL_COPIES $S3_URI $HDFS_DEST_DIR

  # check the exit value. Looking at the DistCp source, exit code -1
  # through -3 are bad news but we can try repeating others. DistCp
  # actually returns -999 for other exceptions.
  RETVAL=$?
  echo "ok - Completed with exit code of $RETVAL"

  # check exit codes
  if [ $RETVAL -eq 0 ]; then
    exit 0
  else
    echo "warn - Detected non-zero status code $RETVAL from distcp"
  fi

  for val in "${BAD_RETURNS[@]}"; do
    if [ $RETVAL -eq $val ]; then
      >&2 echo "Cannot retry.  Quitting. Exiting with status code $RETVAL"
      exit $RETVAL
    fi
  done

  echo "warn - Retrying ..."
  sleep 2

  let COUNTER=COUNTER+1
done

if [ $COUNTER -eq $NUM_RETRIES ] ; then
  >&2 echo "fail - $0 exhausted all retries and still fail, exiting with status code $RETVAL"
  exit $RETVAL
else
  exit 0
fi

