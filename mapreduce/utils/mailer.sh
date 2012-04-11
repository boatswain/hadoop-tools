#!/bin/bash

# para1: subject
# para2: message
# para3: receiver
function SEND_MAIL() {
    if [ $# -ne 3 ];then
        LOG_ERROR "SEND MAIL" "parameters error."
    fi
    echo "${2}" | mail -s "$(date '+%Y-%m-%d %H:%M:%S') ${1}" "${3}"
    mail_ret_code=$?
    if [ ${mail_ret_code} -ne 0 ];then
        LOG_ERROR "SEND MAIL" "send mail error, return code: ${mail_ret_code}"
    else
        LOG_INFO "SEND MAIL" "send mail success, receiver: ${3}"
    fi
}
