#!/bin/sh

mails="xiongdingyun@baidu.com"

/home/iknow/php/bin/php monitor_warning.php
cnt=$?
if [ $cnt -gt 0 ]
then
    /home/iknow/php/bin/php monitor_warning.php | mail -s "[WARNING][`date +%Y%m%d`][��������ͬ����������${cnt}������]" ${mails}
fi

/home/iknow/php/bin/php monitor_notice.php
cnt=$?
/home/iknow/php/bin/php monitor_notice.php | mail -s "[ͬ���б�][`date +%Y%m%d`][��������ͬ����״̬��Ϣ���ܼ�${cnt}�����ݱ�]" ${mails}
