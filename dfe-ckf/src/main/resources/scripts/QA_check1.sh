set -vx


QUEUE_NAME=$1
HIVE_FCST_DB=$2
EMAIL_LIST=$3
SCORE_OUPUT_TABLE=$4
SCORE_INPUT_TABLE=$5

echo ${QUEUE_NAME} ${HIVE_FCST_DB} ${EMAIL_LIST}

sh QA_check0.sh ${HIVE_FCST_DB} ${QUEUE_NAME} ${SCORE_OUPUT_TABLE} ${SCORE_INPUT_TABLE} > test.html

(echo -e "Subject: [DFE] New Item Forecasting QA Report\nMIME-Version: 1.0\nFrom: $MAIL_FROM\nTo:${EMAIL_LIST}\nContent-Type: text/html\nContent-Disposition: inline\n\n";cat test.html) | /usr/sbin/sendmail -f nitin.das@target.com ${EMAIL_LIST}
