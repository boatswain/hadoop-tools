#!/bin/bash

export JAVA_HOME="/home/mapred/hadoop-v2/java6"
HADOOP_HOME="/home/mapred/hadoop-v2/hadoop"
HADOOP_BIN="${HADOOP_HOME}/bin/hadoop --config ./core-site.xml"

if [ -z $zip_input_path ] || [ -z $zip_output_path ];then
    echo "read zip_input_path/zip_output_path cmdenv failed." >&2
    exit 1
fi

# the path going to be compress
echo "zip_input_path=$zip_input_path" >&2
# the compress output path
echo "zip_output_path=$zip_output_path" >&2

# remove right char "/"
zip_input_path=${zip_input_path%/}
# get zip_input_path length
zip_input_path_len=${#zip_input_path}

while read input_idx file_size file_path
do
    echo "file_size: $file_size" >&2
    echo "file_path: $file_path" >&2
    # get file sub path of file_path
    file_sub_path=${file_path:zip_input_path_len}
    compress_output_path="${zip_output_path}/${file_sub_path}.gz"
    echo "compress_output_path: $compress_output_path" >&2
    ${HADOOP_BIN} fs -test -e ${compress_output_path}
    if [ $? -eq 0 ];then
        echo "${compress_output_path} already exists, detele it."
        ${HADOOP_BIN} fs -rm ${compress_output_path}
    fi
    ${HADOOP_BIN} fs -cat ${file_path} | gzip | ${HADOOP_BIN} fs -put - ${compress_output_path}
    pipe_status=${PIPESTATUS[*]}
    if [[ "0 0 0" == ${pipe_status} ]];then
        echo "pipe_status: $pipe_status" >&2
        echo "compress and write success." >&2
        echo "SUCCESS"
    else
        echo "pipe_status: $pipe_status" >&2
        echo "compress and write failed." >&2
        echo "FAILED"
        exit 1
    fi
done
