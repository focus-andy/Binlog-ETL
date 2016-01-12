#!homephp/mall/php/bin/php
<?php

$in = fopen("php://stdin", "r");
$results = array();
while ( $line = fgets($in) ) {
    ## hive_item_key\1hive_item_version\1hive_item_valid\1...(other schema)
    list($key, $ver, $val) = explode("\1", $line, 3);
    print "$key\t$ver\t$val";
}
fclose($in);

