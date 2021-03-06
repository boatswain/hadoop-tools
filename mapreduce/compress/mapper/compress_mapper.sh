#!/bin/bash

export JAVA_HOME="/home/mapred/hadoop-v2/java6"
HADOOP_HOME="/home/mapred/hadoop-v2/hadoop"
HADOOP_BIN="${HADOOP_HOME}/bin/hadoop --config ."

if [ -z $zip_input_path ] || [ -z $zip_output_path ] || [ -z $compress_type ];then
    echo "read zip_input_path/zip_output_path/compress_type cmdenv failed." >&2
    exit 1
fi

# the path going to be compress
echo "zip_input_path=$zip_input_path" >&2
# the compress output path
echo "zip_output_path=$zip_output_path" >&2
# the compress type
echo "compress_type=$compress_type" >&2

# remove right char "/", if it exists.
zip_input_path=${zip_input_path%/}
# get zip_input_path length
zip_input_path_len=${#zip_input_path}

# para1: file_size
# para2: file_path
function compress() {
    # get file sub path of file_path
    file_sub_path=${file_path:zip_input_path_len}
    file_sub_path=${file_sub_path#/}

    # generate compress output path.
    if [ $compress_type == "file" ];then
        compress_output_path="${zip_output_path}/${file_sub_path}.gz"
    elif [ $compress_type == "folder" ];then
        compress_output_path="${zip_output_path}/${file_sub_path}.tar.gz"
    else
        echo "unkown compress type, $compress_type" >&2
        exit 1
    fi
    compress_output_path_tmp="${compress_output_path}.${RANDOM}.${RANDOM}.tmp~~"
    echo "compress_output_path: ${compress_output_path}" >&2
    echo "compress_output_path_tmp: ${compress_output_path_tmp}" >&2

    # check compress output is exists.
    ${HADOOP_BIN} fs -test -e ${compress_output_path}
    if [ $? -eq 0 ];then
        echo "${compress_output_path} already exists, exit." >&2
        echo "SUCCESS"
        exit 0
        # we suppose compress has completed, when output file already exists.???
        # exit 1
    fi

    has_error=0
    if [ $compress_type == "file" ];then
        ${HADOOP_BIN} fs -cat ${file_path} | gzip | ${HADOOP_BIN} fs -put - ${compress_output_path_tmp} >&2
        pipe_status=${PIPESTATUS[*]}
        echo "compress pipe_status: $pipe_status" >&2
        if [[ "0 0 0" != ${pipe_status} ]];then
            ${HADOOP_BIN} fs -rmr ${compress_output_path_tmp}
            has_error=1
        fi
    else
        ${HADOOP_BIN} fs -get ${file_path} ${file_sub_path} >&2
        test $? -eq 0 || has_error=1
        tar cvzf ${file_sub_path}.tar.gz ${file_sub_path} >&2
        test $? -eq 0 || has_error=1
        ${HADOOP_BIN} fs -put ${file_sub_path}.tar.gz ${compress_output_path_tmp} >&2
        test $? -eq 0 || has_error=1
        rm -rf ${file_sub_path}
    fi

    if [ $has_error -eq 0 ];then
        echo "compress and write tmp file success." >&2
        ${HADOOP_BIN} fs -mv ${compress_output_path_tmp} ${compress_output_path} >&2
        mv_ret=$?
        if [ ${mv_ret} -ne 0 ];then
            echo "mv ${compress_output_path_tmp} to ${compress_output_path} failed, mv_ret: ${mv_ret}" >&2
            echo "FAILED"
            ${HADOOP_BIN} fs -rmr ${compress_output_path_tmp}
            exit 1
        fi
        echo "mv ${compress_output_path_tmp} to ${compress_output_path} success." >&2
        echo "SUCCESS"
        echo "compress and write success." >&2
    else
        echo "compress and write tmp file failed." >&2
        echo "FAILED"
        exit 1
    fi
}

while read input_idx file_size file_path
do
    echo "file_size: $file_size" >&2
    echo "file_path: $file_path" >&2
    compress "$file_size" "$file_path"
done
