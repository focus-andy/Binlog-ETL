<?php


$dt=date("Ymd", strtotime("now"));
if (!exec ("find ../data/ -name 'exec.log.$dt'", $result, $ret) ) {
    echo "Monitor data_dir not exist\n";
    exit(0);
}
echo "ALL tables of sync: " . count($result) . "\n";
$cnt = count($result);
foreach ($result as $one) {
    $data = array();
    exec ("tail -3 $one", $data, $ret);
    echo $one."\n";
    foreach ($data as $row) {
        echo "\t$row\n";
    }
}
exit($cnt);
