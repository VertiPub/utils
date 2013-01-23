#!/bin/bash

# puts a simple wrapper shell script around hadoop distcp to retry in
# case of certain exit codes. This is intended to copy from hdfs to
# S3.

# soam@verticloud.com
# VertiCloud Inc.

NUM_RETRIES=8

# limit the number of copies running at the same time
MAX_PARALLEL_COPIES=5

# input directory name. No trailing slash eg. /a/b
HDFS_INPUT_DIR=

# location of destination bucket eg. bucket_foo
S3_DEST_BUCKET=

# location where the data will be placed in S3. eg. /a/b
S3_DEST_DIR=


# assumes the AWS access and secret keys are defined in the shell environment
MY_AWS_ACCESS_KEY=${AWS_ACCESS_KEY}
MY_AWS_SECRET_KEY=${AWS_SECRET_KEY}
S3_URI=s3n://$MY_AWS_ACCESS_KEY:$MY_AWS_SECRET_KEY@$S3_DEST_BUCKET$S3_DEST_DIR

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
	echo "Completed"
	exit
    fi
    # -1
    if [ $RETVAL -eq 255 ]; then
        echo "Quiting. Can't retry."
        exit
    fi
    
    # -2
    if [ $RETVAL -eq 254 ]; then
        echo "Quitting. Can't retry."
        exit
    fi

    # -3
    if [ $RETVAL -eq 253 ]; then
        echo "Quitting. Can't retry."
        exit
    fi

    echo "Retrying ..."
    sleep 2

    let COUNTER=COUNTER+1
done
