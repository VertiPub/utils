#!/usr/bin/env bash

# Verify number of arguments
if [[ $# -ne 1 ]] ; then
  echo "usage: sparkenv [spark1.6|spark1.5]"
fi

# Set Spark environment
if [[ $# -eq 1 ]] ; then
  case $1 in

    "spark1.6" | "spark")
      export SPARK_HOME=/opt/spark
      export SPARK_CONF_DIR=/etc/spark
      export SPARK_VERSION="1.6.1" ;;

    "spark1.5")
      export SPARK_HOME=/opt/spark15
      export SPARK_CONF_DIR=/etc/spark15
      export SPARK_VERSION="1.5.2" ;;

    *) echo "spark unknown version: $1 != spark1.6 | spark1.5" ;;
  esac
fi

# Print environment 

if [[ $quiet != 'true' ]]; then
  echo -n 'Spark environment is '
  if [[ "x$SPARK_HOME" == "x" ]] ; then
    echo 'not set'
  else
    # If no arguments passed, print the current Spark environment
    echo "$SPARK_VERSION"
    echo "SPARK_HOME -> $SPARK_HOME"
    echo "SPARK_CONF_DIR -> $SPARK_CONF_DIR"
  fi
fi 
