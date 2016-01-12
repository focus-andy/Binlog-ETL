#!homephp/mall/php/bin/php
<?php

$in = fopen("php://stdin","r");
$unqkey = "";
$version = "";
$value = "";
while ( $line = fgets($in) ) {
    list($key, $ver, $val) = explode("\t", $line, 3);
    if($unqkey!=$key) {
        if($unqkey!="") { print $value; }
        $unqkey=$key;
        $version= $ver;
        $value = $key . "\1" . $ver. "\1" . $val;
    } else {
		if( strcmp($ver, $version) > 0 ){
            $version= $ver;
            $value = $key . "\1" . $ver. "\1" . $val;
        }
    }
}
fclose($in);

if($unqkey!="") { print $value; }

