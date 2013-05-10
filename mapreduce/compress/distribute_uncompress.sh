#!/bin/bash
#
# Gzip compress HDFS Files use MapReduce.
# Notice: 
#    1. Compress output directory shouldn't supposed to exists.
#    2. core-site.xml should be provided and must speicify property of hadoop.job.ugi and fs.default.name.
#    3. make sure hdfs://clustername/tmp/uncompress_tmp/ has exists, and has read-write permissions.
#
#
# For Example:
#    1. uncompress by file
#    input hdfs path tree:
#    /compress
#    ├── 00.log.gz
#    ├── 01.log.gz
#    ├── 02.log.gz
#    ├── subdir
#    │   └── 00.log.gz
#    │   └── 01.log.gz
#
#    output hdfs path tree:
#    /log
#    ├── 00.log
#    ├── 01.log
#    ├── 02.log
#    ├── subdir
#    │   └── 00.log
#    │   └── 01.log
#
#    2. uncompress by folder
#    input hdfs path tree:
#    /compress
#    ├── 00.log.tar.gz
#    ├── 01.log.tar.gz
#    ├── 02.log.tar.gz
#    ├── subdir.tar.gz
#
#    output hdfs path tree:
#    /log
#    ├── 00.log
#    ├── 01.log
#    ├── 02.log
#    ├── subdir
#    │   └── 00.log
#    │   └── 01.log
#
# Author: fengzanfeng@wandoujia.com
# Site: 
#

source ../utils/init.sh
source ../utils/logger.sh
source ../utils/mailer.sh

LOG_INIT "distribute_uncompress"

source ./compress-base.sh

# the hdfs path for write lsr list file.
MAPRED_JOB_TMP_DIR="/tmp/uncompress_tmp/"

# the local path for wrrite lsr list file.
LOCAL_LSR_TMP_FILE="file_list_${RANDOM}_`date +%Y%m%d%H%M%S`"

# compress type, file or folder
COMPRESS_TYPE="file"

# the hdfs for write mapred task status.
MAPRED_OUTPUT_DIR="${MAPRED_JOB_TMP_DIR}/${LOCAL_LSR_TMP_FILE}_output"

# mapred job name
JOB_NAME="distribute_uncompress-$(date '+%Y%m%d%H%M%S')"

function usage() {
    echo ""
    echo "./distribute_uncompress.sh <input path> <output path> [compress type] [mapred job capacity] [mapred job priority]" >&2
    echo "input path:              input path ready going to be uncompress" >&2
    echo "output path:             where the uncompressed file storage" >&2
    echo "compress type:           file or folder" >&2
    echo "mapred job capacity:     mapred job capacity. 3 (default)" >&2
    echo "mapred job priority:     mapred job priority. VERY_LOW (default)" >&2
    exit 1
}

# check main parameter
if [ $# -lt 2 ];then
    usage
fi

# the path being compress
INPUT_PATH=$1
INPUT_PATH=${INPUT_PATH%/}
# the path for compress output
OUTPUT_PATH=$2
OUTPUT_PATH=${OUTPUT_PATH%/}

if [ ! -z $3 ];then
    COMPRESS_TYPE=$3
fi
JOB_NAME=${JOB_NAME}-${COMPRESS_TYPE}

if [ ! -z $4 ];then
    COMPRESS_FORMAT=$4
fi

# mapreduce job para
MAP_TASK_CAPACITY=3
JOB_PRIORITY="VERY_LOW"

if [ ! -z $5 ];then
    MAP_TASK_CAPACITY=$5
fi

if [ ! -z $6 ];then
    JOB_PRIORITY=$6
fi

LOG_INFO "distribute uncompress" "distribute uncompress starts..."
LOG_INFO "distribute uncompress" "jobname: $JOB_NAME"
LOG_INFO "distribute uncompress" "local lsr file: $LOCAL_LSR_TMP_FILE"
LOG_INFO "distribute uncompress" "compress type: $COMPRESS_TYPE"

# check hdfs input/output path
check_mapred_input_output "${JOB_NAME}" "${INPUT_PATH}" "${OUTPUT_PATH}"

# recursive list hdfs input path and storage file list to local, then upload to hdfs
recursive_list_hdfs_path "${INPUT_PATH}" "${LOCAL_LSR_TMP_FILE}" "${MAPRED_JOB_TMP_DIR}" "${COMPRESS_TYPE}"

# start job
LOG_INFO "submit mapreduce job" "distribute uncompress mapreduce job starts..."
${HADOOP_HOME}/bin/hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming-1.0.2-w1.2.0.jar \
    -D mapred.job.name="${JOB_NAME}" \
    -D mapred.job.map.capacity=${MAP_TASK_CAPACITY} \
    -D mapred.job.priority=${JOB_PRIORITY} \
    -D mapred.line.input.format.linespermap=1 \
    -D mapred.map.tasks.speculative.execution=false \
    -input "${MAPRED_JOB_TMP_DIR}/${LOCAL_LSR_TMP_FILE}" \
    -output "${MAPRED_OUTPUT_DIR}" \
    -inputformat "org.apache.hadoop.mapred.lib.NLineInputFormat" \
    -mapper "bash uncompress_mapper.sh" \
    -reducer "NONE" \
    -file "mapper/uncompress_mapper.sh" \
    -file "core-site.xml" \
    -cmdenv zip_input_path=${INPUT_PATH} \
    -cmdenv zip_output_path=${OUTPUT_PATH} \
    -cmdenv compress_type=${COMPRESS_TYPE}

ret_code=$?
if [ ${ret_code} -ne 0 ];then
    LOG_FATAL "distribute uncompress" "${JOB_NAME} uncompress job failed."
    SEND_MAIL "distribute uncompress failed" "${JOB_NAME} uncompress job failed. retcode=${ret_code}" ${MAIL_LIST}
    LOG_SEPERATOR
    exit 1
else
    LOG_INFO "distribute uncompress" "${JOB_NAME} uncompress job success."
fi

# check result
check_mapred_result ${JOB_NAME} ${LOCAL_LSR_TMP_FILE} ${OUTPUT_PATH} ${MAPRED_OUTPUT_DIR} ${COMPRESS_TYPE}
LOG_INFO "main" "distribute uncompress completed success."
LOG_SEPERATOR
