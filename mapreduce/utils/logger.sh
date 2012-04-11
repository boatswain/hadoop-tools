#!/bin/bash

_LOGGER_LOG_NAME=undefind
_LOGGER_LOG_DIR=logs
_LOGGER_LOG_PATH=undefind.log



# para1: log name
# para2: log dir [optinal]
function LOG_INIT() {
    if [ $# -eq 0 ];then
        echo "Log Init Parameter Error." >&2
        exit 1
    fi

    if [ $# -ge 1 ];then
        _LOGGER_LOG_NAME="$1"
    fi

    if [ $# -ge 2 ];then
        _LOGGER_LOG_DIR="$2"
    fi

    if [ ! -d $_LOGGER_LOG_DIR ];then
        mkdir $_LOGGER_LOG_DIR
        if [ $? -ne 0 ];then
            echo "Log Init Error, mkdir log dir ${_LOGGER_LOG_DIR} error." >&2
            exit 1
        fi
    fi
    _LOGGER_LOG_PATH=${_LOGGER_LOG_DIR}/${_LOGGER_LOG_NAME}.log
    touch ${_LOGGER_LOG_PATH}
}

# para1: log header
# para2: log body
function LOG_DEBUG() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [DEBUG] [${1}] ${2}" >>${_LOGGER_LOG_PATH}
}


# para1: log header
# para2: log body
function LOG_INFO() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] [${1}] ${2}" >>${_LOGGER_LOG_PATH}
}


# para1: log header
# para2: log body
function LOG_ERROR() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [ERROR] [${1}] ${2}" >>${_LOGGER_LOG_PATH}
}


# para1: log header
# para2: log body
function LOG_FATAL() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [FATAL] [${1}] ${2}" >>${_LOGGER_LOG_PATH}
}


# para1: log header
# para2: log body
function LOG_CRITIC() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [CRITIC] [${1}] ${2}" >>${_LOGGER_LOG_PATH}
}

function LOG_SEPERATOR() {
    echo "=============================================================" >>${_LOGGER_LOG_PATH}
}
