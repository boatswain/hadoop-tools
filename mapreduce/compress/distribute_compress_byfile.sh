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

export JAVA_HOME="$HOME/hadoop-v2/java6"
MAIL_LIST="fengzanfeng@wandoujia.com,zhoupo@wandoujia.com"
HADOOP_HOME="$HOME/hadoop-v2/hadoop"
HADOOP_BIN="${HADOOP_HOME}/bin/hadoop --config ./core-site.xml"
MAPRED_JOB_TMP_DIR="/tmp/compress_tmp/"
LOCAL_LSR_TMP_FILE="file_list_${RANDOM}_`date +%Y%m%d%H%M%S`"
MAPRED_OUTPUT_DIR="${MAPRED_JOB_TMP_DIR}/${LOCAL_LSR_TMP_FILE}_output"
COMPRESS_FORMAT="gzip"
DELETE_SOURCE="false"
TODAY=`date "+%Y-%m-%d"`



function usage() {
    echo ""
    echo "./distribute_compress_byfile.sh <input path> <output path> <delete source> [compress format] [mapred job capacity] [mapred job priority]" >&2
    echo "input path:              input path ready going to be compress" >&2
    echo "output path:             where the compressed file storage" >&2
    echo "delete source:           delete input soure where compress success" >&2
    echo "compress format:         gzip. gzip(default)" >&2
    echo "mapred job capacity:     mapred job capacity. 3 (default)" >&2
    echo "mapred job priority:     mapred job priority. VERY_LOW (default)" >&2
    exit 1
}

# para1: hdfs input path    para2: hdfs output path
function check_input_output() {
    ${HADOOP_BIN} fs -test -e $1
    if [ $? -ne 0 ];then
        echo "[FATAL] input path $1 not exists, exit..." >&2
        exit 1
    fi

    ${HADOOP_BIN} fs -test -e $2
    if [ $? -eq 0 ];then
        echo "[FATAL] output path $2 shouldn't be exists, exit..." >&2
        exit 1
    fi
}

# para1: hdfs input path
function recursive_list() {
    ${HADOOP_BIN} fs -lsr $1 | grep '^\-r' | awk '{print $5,$8}' >$LOCAL_LSR_TMP_FILE
    if [ $? -ne 0 ];then
        echo "recursive list input path $1 failed, exit..." >&2
        exit 1
    fi
    FILE_NUM=$(cat $LOCAL_LSR_TMP_FILE | wc -l)
    if [ $FILE_NUM -le 0 ];then
        echo "recursive list input file num is 0, exit..." >&2
        exit 1
    fi
    ${HADOOP_BIN} fs -put $LOCAL_LSR_TMP_FILE $MAPRED_JOB_TMP_DIR/$LOCAL_LSR_TMP_FILE
    if [ $? -ne 0 ];then
        echo "write $LOCAL_LSR_TMP_FILE to $MAPRED_JOB_TMP_DIR failed." >&2
        exit 1
    fi
}

# para1: compress format
function check_compress_format() {
    if [ $1 != "gzip" ];then
        echo "nor supported compress fomrat. current only gzip is supported."
        exit 1
    fi
}

function check_result() {
    INPUT_FILE_NUM=$(cat $LOCAL_LSR_TMP_FILE | wc -l)
    OUTPUT_FILE_NUM=$(${HADOOP_BIN} fs -lsr ${OUTPUT_PATH} | grep '^\-r' | wc -l)
    echo "input file num: ${INPUT_FILE_NUM}, output file num: ${OUTPUT_FILE_NUM}"
    if [ ${INPUT_FILE_NUM} -ne ${OUTPUT_FILE_NUM} ];then
        echo "[FATAL] input file num not equals output file num."
        echo "`date '+%Y-%m-%d %H:%M:%S'` run compress job failed. retcode=${ret_code}" | mail -s "compress job failed." ${MAIL_LIST}
        exit 1
    fi

    SUCCESS_TASK_NUM=$(${HADOOP_BIN} fs -cat ${MAPRED_OUTPUT_DIR}/part-* | grep 'SUCCESS' | wc -l)
    echo "expected sucess num: ${INPUT_FILE_NUM} actual success num: ${SUCCESS_TASK_NUM}"
    if [ ${SUCCESS_TASK_NUM} -ne ${INPUT_FILE_NUM} ];then
        echo "[FATAL] some task failed, please check ${MAPRED_OUTPUT_DIR}"
        echo "`date '+%Y-%m-%d %H:%M:%S'` run compress job failed. retcode=${ret_code}" | mail -s "compress job failed." ${MAIL_LIST}
        exit 1
    fi
}

function delete_source() {
    if [ $DELETE_SOURCE == "true" ];then
        echo "delete input source: ${INPUT_PATH} starts..."
        ${HADOOP_BIN} fs -rmr ${INPUT_PATH}
        echo "delete input source: ${INPUT_PATH} success"
    fi
}

# check parameter
if [ $# -lt 3 ];then
    usage
fi

# the path being compress
INPUT_PATH=$1
# the path for compress output
OUTPUT_PATH=$2
# delete input source
DELETE_SOURCE=#3

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

# check hdfs input/output path
check_input_output ${INPUT_PATH} ${OUTPUT_PATH}

# list input directory
recursive_list ${INPUT_PATH}

# start job
echo "`date '+%Y-%m-%d %H:%M:%S'` submit mapreduce job"
${HADOOP_HOME}/bin/hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming-v2.1-0.20.205.0.jar \
    -D mapred.job.name="distribute_compress_byfile-${TODAY}" \
    -D mapred.job.map.capacity=${MAP_TASK_CAPACITY} \
    -D mapred.job.priority=${JOB_PRIORITY} \
    -D mapred.line.input.format.linespermap=1 \
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
    echo "`date '+%Y-%m-%d %H:%M:%S'` run job failed."
    echo "`date '+%Y-%m-%d %H:%M:%S'` run compress job failed. retcode=${ret_code}" | mail -s "compress job failed." ${MAIL_LIST}
    exit 1
else
    echo "`date '+%Y-%m-%d %H:%M:%S'` run job successful"
fi

# check result, exit with 1 when check failed.
check_result

# delete input source when compress successful.
delete_source
