#!/usr/bin/env bash
# properties = {properties}

# print cluster job id
echo "Running cluster job $JOB_ID"
echo "-----------------------------"

# run the job command
( {exec_job} )
EXIT_STATUS=$?  # get the exit status

# print resource consumption
echo "-----------------------------"
qstat -j $JOB_ID | grep '^usage'

# print exit status
echo "-----------------------------"
echo "EXIT_STATUS: $EXIT_STATUS"

# save the exit status to file
CLUSTER_DIR="{{cookiecutter.cluster_dir}}"
mkdir -p "$CLUSTER_DIR"
echo "$EXIT_STATUS" >> "$CLUSTER_DIR/$JOB_ID.exit"

# exit with captured exit status
exit $EXIT_STATUS
