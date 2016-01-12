<?php


$dt=date("Ymd", strtotime("now"));
$retn = 0;
if (exec ( "find ../data/ -name 'exec.log.wf.$dt'", $result, $ret) ) {
    echo "Errors in tables: " . count($result) . "\n";
    foreach ($result as $one) {
        $data = array();
        exec ("tail -3 $one", $data, $ret);
        echo $one."\n";
        foreach ($data as $row) {
            echo "\t$row\n";
        }
    }
    $retn += count($result);
}

if (exec ("ps aux |grep -P '/bin/sh.*sync.binlog2dw.*tbl=(\d+)'|grep -oP 'php sync.binlog2dw.*'", $threads, $ret) ) {
    echo "\nSync threads running now: " . count($threads) . "\n";
    foreach ($threads as $one) {
        if (preg_match("/sync.binlog2dw.php tbl=(\d+)/", $one, $matches)) {
            require_once ('../conf/sync.conf.php');
            if (isset ($GLOBALS['sync_config']['configs'][$matches[1]])) {
                $one = $one . "  ==> table_info: " . $GLOBALS['sync_config']['configs'][$matches[1]]['dbname'] 
                    . "_" . $GLOBALS['sync_config']['configs'][$matches[1]]['tblname'];
            }
        }
        echo "\t$one\n";
    }
    $retn += count($threads);
}

exit($retn);
