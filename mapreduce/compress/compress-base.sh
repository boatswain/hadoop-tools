#!/bin/bash

MAIL_LIST="fengzanfeng@wandoujia.com,zhoupo@wandoujia.com,wanghai@wandoujia.com,wangyuzhou@wandoujia.com"
HADOOP_BIN="${HADOOP_HOME}/bin/hadoop --config ."
TODAY=`date "+%Y-%m-%d"`

# para1: job name, only for logger
# para2: hdfs input path
# para3: hdfs output path
function check_mapred_input_output() {
    LOG_INFO "CHECK MAPRED INPUT OUTPUT" "check mapred job input output starts..."
    LOG_INFO "CHECK MAPRED INPUT OUTPUT" "input: $2 output: $3"
    if [ $# -ne 3 ];then
        LOG_FATAL "CHECK MAPRED INPUT OUTPUT" "parameter error."
        LOG_SEPERATOR
        exit 1
    fi
    ${HADOOP_BIN} fs -test -e $2
    if [ $? -ne 0 ];then
        LOG_FATAL "CHECK MAPRED INPUT OUTPUT" "[$1] input path $2 not exists, exits..."
        SEND_MAIL "CHECK MAPRED INPUT OUTPUT" "[$1] input path $2 not exists" ${MAIL_LIST}
        LOG_SEPERATOR
        exit 1
    fi

    ${HADOOP_BIN} fs -test -e $3
    if [ $? -eq 0 ];then
        LOG_FATAL "CHECK MAPRED INPUT OUTPUT" "[$1] output path $3 shouldn't be exists, exits..."
        SEND_MAIL "CHECK MAPRED INPUT OUTPUT ERROR" "[$1]output path $3 shouldn't be exists" ${MAIL_LIST}
        LOG_SEPERATOR
        exit 1
    fi
    LOG_INFO "CHECK MAPRED INPUT OUTPUT" "check mapred job input output success."
}


# para1: hdfs input path
# para2: local lsr file
# para3: mapred job tmp path
# para4: compress type          file/folder
function recursive_list_hdfs_path() {
    LOG_INFO "RECURSIVE LIST HDFS PATH" "recursive list starts..."
    has_error=0
    if [ "$4" == "file" ];then
        ${HADOOP_BIN} fs -lsr $1 | grep '^-r' | awk '{print $5,$8}' >$2
        test $? -eq 0 || has_error=1
    elif [ "$4" == "folder" ];then
        ${HADOOP_BIN} fs -ls $1 | grep -E '^-|^d' | awk '{print $5,$8}' >$2
        test $? -eq 0 || has_error=1
    else
        LOG_ERROR "RECURSIVE LIST HDFS PATH" "unknown compress type, should be file/folder"
        SEND_MAIL "RECURSIVE LIST HDFS PATH ERROR" "unknown compress type, should be file/folder" ${MAIL_LIST}
        exit 1
    fi

    if [ $has_error -ne 0 ];then
        LOG_ERROR "RECURSIVE LIST HDFS PATH" "recursive list failed, exits..."
        SEND_MAIL "RECURSIVE LIST HDFS PATH ERROR" "recursive list input path $1 failed." ${MAIL_LIST}
        LOG_SEPERATOR
        exit 1
    fi

    FILE_NUM=$(cat $2 | wc -l)
    if [ "$FILE_NUM" -le 0 ];then
        LOG_ERROR "RECURSIVE LIST HDFS PATH" "recursive list input file num is 0, exits..."
        SEND_MAIL "RECURSIVE LIST HDFS PATH ERROR" "recursive list input file num is 0." ${MAIL_LIST}
        LOG_SEPERATOR
        exit 1
    fi

    ${HADOOP_BIN} fs -put $2 $3/$(basename $2)
    if [ $? -ne 0 ];then
        LOG_INFO "RECURSIVE LIST HDFS PATH" "upload $2 to hdfs failed."
        SEND_MAIL "RECURSIVE LIST HDFS PATH ERROR" "upload $2 to hdfs failed." ${MAIL_LIST}
        LOG_SEPERATOR
        exit 1
    fi
    LOG_INFO "RECURSIVE LIST HDFS PATH" "recursive list success."
}


# para1: job name, only for logger
# para2: local lsr file
# para3: compress or uncompress result path
# para4: mapred job output path, where to storage job status.
# para5: compress_type  file/folder
function check_mapred_result() {
    INPUT_FILE_NUM=$(cat $2 | wc -l)
    if [ "$5" == "file" ];then
        OUTPUT_FILE_NUM=$(${HADOOP_HOME}/bin/hadoop fs -lsr $3 | grep -v '~~' | grep '^-r' | wc -l)
        LOG_INFO "CHECK MAPRED RESULT" "check job result starts."
        LOG_INFO "CHECK MAPRED RESULT" "input file num: ${INPUT_FILE_NUM}, output file num: ${OUTPUT_FILE_NUM}"
        if [ ${INPUT_FILE_NUM} -ne ${OUTPUT_FILE_NUM} ];then
            LOG_FATAL "CHECK MAPRED RESULT" "$1 input file num not equals output file num."
            SEND_MAIL "CHECK MAPRED RESULT ERROR" "$1 input file num not equals output file num." ${MAIL_LIST}
            LOG_SEPERATOR
            exit 1
        fi
    fi

    SUCCESS_TASK_NUM=$(${HADOOP_HOME}/bin/hadoop fs -cat ${4}/part-* | grep 'SUCCESS' | wc -l)
    LOG_INFO "CHECK MAPRED RESULT" "expected sucess num: ${INPUT_FILE_NUM} actual success num: ${SUCCESS_TASK_NUM}"
    if [ "${SUCCESS_TASK_NUM}" -ne "${INPUT_FILE_NUM}" ];then
        LOG_FATAL "CHECK MAPRED RESULT" "$1 some task failed, please check $4"
        SEND_MAIL "MAPRED RESULT ERROR" "$1 some task failed, please check $4" ${MAIL_LIST}
        LOG_SEPERATOR
        exit 1
    fi

    ${HADOOP_HOME}/bin/hadoop fs -lsr ${3} | grep '~~'
    if [ $? -eq 0 ];then
        LOG_INFO "CHECK MAPRED RESULT" "$1 some tmp files exists in compressed path: ${3}, maybe you can delete all tmp files."
        SEND_MAIL "MAPRED RESULT NOTICE" "$1 some tmp files exists in $3" $MAIL_LIST
        exit 1
    fi

}
