#!/bin/bash

export JAVA_HOME="/home/mapred/hadoop-v2/java6"
HADOOP_HOME="/home/mapred/hadoop-v2/hadoop"
HADOOP_BIN="${HADOOP_HOME}/bin/hadoop --config ./core-site.xml"

if [ -z $zip_input_path ] || [ -z $zip_output_path ];then
    echo "read zip_input_path/zip_output_path cmdenv failed." >&2
    exit 1
fi

# the path going to be uncompress
echo "zip_input_path=$zip_input_path" >&2
# the uncompress output path
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
    uncompress_output_path="${zip_output_path}/${file_sub_path}"
    uncompress_output_path=${uncompress_output_path%.gz}
    uncompress_output_path_tmp="${uncompress_output_path}.${RANDOM}.tmp~~"
    echo "uncompress_output_path: ${uncompress_output_path}" >&2
    echo "uncompress_output_path_tmp: ${uncompress_output_path_tmp}" >&2
    ${HADOOP_BIN} fs -test -e ${uncompress_output_path}
    if [ $? -eq 0 ];then
        echo "${uncompress_output_path} already exists, exit." >&2
        echo "SUCCESS"
        exit 0
        # when dst file already has existsed, we think thas is successful.???
        # exit 1
    fi
    ${HADOOP_BIN} fs -cat ${file_path} | gzip -d | ${HADOOP_BIN} fs -put - ${uncompress_output_path_tmp} >&2
    pipe_status=${PIPESTATUS[*]}
    echo "pipe_status: $pipe_status" >&2
    if [[ "0 0 0" == ${pipe_status} ]];then
        echo "uncompress and write tmp file success." >&2
        ${HADOOP_BIN} fs -mv ${uncompress_output_path_tmp} ${uncompress_output_path} >&2
        mv_ret=$?
        if [ ${mv_ret} -ne 0 ];then
            echo "mv ${uncompress_output_path_tmp} to ${uncompress_output_path} failed, mv_ret: ${mv_ret}" >&2
            echo "FAILED"
            exit 1
        fi
        echo "mv ${uncompress_output_path_tmp} to ${uncompress_output_path} success." >&2
        echo "uncompress and write success." >&2
        echo "SUCCESS"
    else
        echo "uncompress and write failed." >&2
        echo "FAILED"
        exit 1
    fi
done
