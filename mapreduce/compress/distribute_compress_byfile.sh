#!/bin/bash
#
# Gzip compress HDFS Files use MapReduce.
# Notice: 
#    1. Compress output directory shouldn't supposed to exists.
#    2. core-site.xml should be provided and must speicify property of hadoop.job.ugi and fs.default.name.
#    3. make sure hdfs://clustername/tmp/compress_tmp/ has exists, and has read-write permissions.
#
#
# For Example:
#    input hdfs path tree:
#    /log
#    ├── 00.log
#    ├── 01.log
#    ├── 02.log
#    ├── subdir
#    │   └── 00.log
#    │   └── 01.log
#
#    output hdfs path tree:
#    /compress
#    ├── 00.log.gz
#    ├── 01.log.gz
#    ├── 02.log.gz
#    ├── subdir
#    │   └── 00.log.gz
#    │   └── 01.log.gz
#
# Author: fengzanfeng@wandoujia.com
# Site: 
#

source ../utils/init.sh
source ../utils/logger.sh
source ../utils/mailer.sh

LOG_INIT "distribute_compress_byfile"

source ./base.sh

# the hdfs path for write lsr list file.
MAPRED_JOB_TMP_DIR="/tmp/compress_tmp/"

# the local path for wrrite lsr list file.
LOCAL_LSR_TMP_FILE="file_list_${RANDOM}_`date +%Y%m%d%H%M%S`"

# the hdfs for write mapred task status.
MAPRED_OUTPUT_DIR="${MAPRED_JOB_TMP_DIR}/${LOCAL_LSR_TMP_FILE}_output"

# only gzip format is supported current.
COMPRESS_FORMAT="gzip"

# does not delete compress input path by default
DELETE_SOURCE="false"

# mapred job name
JOB_NAME="distribute_compress_byfile-$(date '+%Y%m%d%H%M%S')"



function usage() {
    echo ""
    echo "./distribute_compress_byfile.sh <input path> <output path> <delete source> [compress format] [mapred job capacity] [mapred job priority]" >&2
    echo "input path:              input path ready going to be compress" >&2
    echo "output path:             where the compressed file storage" >&2
    echo "delete source:           delete input soure where compress success. false/true" >&2
    echo "compress format:         gzip. gzip(default) [optinal]" >&2
    echo "mapred job capacity:     mapred job capacity. 3 (default) [optinal]" >&2
    echo "mapred job priority:     mapred job priority. VERY_LOW (default) [optinal]" >&2
    exit 1
}

# para1: compress format (only gzip format is supported current.)
function check_compress_format() {
    if [ $1 != "gzip" ];then
        LOG_FATAL "compress format error" "not supported compress fomrat. current only gzip is supported."
        LOG_SEPERATOR
        exit 1
    fi
}

# when mapred job run completed, will decide delete input path.
function delete_source() {
    if [ $DELETE_SOURCE == "true" ];then
        LOG_INFO "delete input source" "delete input path: ${INPUT_PATH} starts..."
        ${HADOOP_BIN} fs -rmr ${INPUT_PATH}
        LOG_INFO "delete input source" "delete input path: ${INPUT_PATH} success"
    else
        LOG_INFO "delete input source" "skip..."
    fi
}

# check main parameter
if [ $# -lt 3 ];then
    usage
fi

# the path being compress
INPUT_PATH=$1
# the path for compress output
OUTPUT_PATH=$2
# delete input source
DELETE_SOURCE=$3
if [ $DELETE_SOURCE != "false" ] && [ $DELETE_SOURCE != "true" ];then
    LOG_INFO "distribute_compress" "delete_source parameter format error."
    LOG_SEPERATOR
    exit 1
fi

if [ ! -z $4 ];then
    COMPRESS_FORMAT=$4
fi
check_compress_format ${COMPRESS_FORMAT}

# mapreduce job para
MAP_TASK_CAPACITY=3
JOB_PRIORITY="VERY_LOW"
if [ ! -z $5 ];then
    MAP_TASK_CAPACITY=$5
fi

if [ ! -z $6 ];then
    JOB_PRIORITY=$6
fi

LOG_INFO "distribute_compress" "distribute_compress starts..."
LOG_INFO "distribute compress" "jobname: $JOB_NAME"
LOG_INFO "distribute compress" "local lsr file: $LOCAL_LSR_TMP_FILE"

# check hdfs input/output path
check_mapred_input_output "${JOB_NAME}" ${INPUT_PATH} ${OUTPUT_PATH}

# recursive list hdfs input path and storage file list to local, then upload to hdfs
recursive_list_hdfs_path ${INPUT_PATH} ${LOCAL_LSR_TMP_FILE} ${MAPRED_JOB_TMP_DIR}

# start job
LOG_INFO "submit mapreduce job" "submit mapreduce job starts..."
${HADOOP_HOME}/bin/hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming-v2.1-0.20.205.0.jar \
    -D mapred.job.name="${JOB_NAME}" \
    -D mapred.job.map.capacity=${MAP_TASK_CAPACITY} \
    -D mapred.job.priority=${JOB_PRIORITY} \
    -D mapred.line.input.format.linespermap=1 \
    -D mapred.map.tasks.speculative.execution=false \
    -input "${MAPRED_JOB_TMP_DIR}/${LOCAL_LSR_TMP_FILE}" \
    -output "${MAPRED_OUTPUT_DIR}" \
    -inputformat "org.apache.hadoop.mapred.lib.NLineInputFormat" \
    -mapper "./compress_byfile_mapper.sh" \
    -reducer "NONE" \
    -file "mapper/compress_byfile_mapper.sh" \
    -file "core-site.xml" \
    -cmdenv zip_input_path=${INPUT_PATH} \
    -cmdenv zip_output_path=${OUTPUT_PATH} \
    -cmdenv compress_format=${COMPRESS_FORMAT}

ret_code=$?
if [ ${ret_code} -ne 0 ];then
    LOG_FATAL "distribute compress" "${JOB_NAME} compress job failed."
    SEND_MAIL "distribute compress failed" "${JOB_NAME} compress job failed. retcode=${ret_code}" ${MAIL_LIST}
    LOG_SEPERATOR
    exit 1
else
    LOG_INFO "distribute compress" "${JOB_NAME} compress job success."
fi

# check result, exit with 1 when check failed.
check_mapred_result ${JOB_NAME} ${LOCAL_LSR_TMP_FILE} ${OUTPUT_PATH} ${MAPRED_OUTPUT_DIR}

# delete input source when compress successful.
delete_source
