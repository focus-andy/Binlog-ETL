#!/bin/sh

mails="xiongdingyun@baidu.com"

/home/iknow/php/bin/php monitor_warning.php
cnt=$?
if [ $cnt -gt 0 ]
then
    /home/iknow/php/bin/php monitor_warning.php | mail -s "[WARNING][`date +%Y%m%d`][离线数据同步出错，存在${cnt}个错误]" ${mails}
fi

/home/iknow/php/bin/php monitor_notice.php
cnt=$?
/home/iknow/php/bin/php monitor_notice.php | mail -s "[同步列表][`date +%Y%m%d`][离线数据同步表状态信息，总计${cnt}个数据表]" ${mails}
