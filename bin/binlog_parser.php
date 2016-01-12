<?php

require_once( '../util/sync.util.php' ) ;

/*
** Usage: Parse binlog file, Convert binlog data to hive daily_inc file
** @Input: $binlog_file
**         $db_name from cfg
**         $tbl_name from cfg
**         $out_file
** @Output: return "the file of out data"
*/
function parse_binlog_rows ($binlog_file, $cfg, $out_file) {
    write2log("parse $binlog_file");
    $fd = fopen($binlog_file, "r");
    $unix_time_update = 0;
	$db_name = $cfg['dbname'] ;
	$tbl_name = $cfg['tblname'] ;
	$dbtbl_parttern = $cfg['db_tbl_pattern'] ;
	$hive_keys = $cfg['hive_item_key'] ;

    $tbl_pattern = preg_replace("/\%d/", "\d+", $dbtbl_parttern);
    $_pattern = sprintf ($tbl_pattern, $db_name, $tbl_name);
    while (!feof($fd)) {
        $line = "";
        $line = fgets($fd);
        if (empty($line)) {
            break;
        }
        while (!feof($fd) && preg_match ("/^### /", $line, $matches)) {
            if (preg_match ("/^### (UPDATE|INSERT INTO) (${_pattern})$/", $line, $matches)) {
                $set_flag = false;
                $hive_item_key = trim ($matches[2]);
				$db_tbl_str = trim( $matches[2] ) ;
                while (!feof($fd)) {
                    $line = fgets($fd);
                    if(preg_match ("/^### SET$/", $line, $matches)) {
                        $set_flag = true;
                        break;
                    }
                    if(!preg_match ("/^### /", $line, $matches)) {
                        break;
                    }
                }
                $out_line = "$unix_time_update\1" . 0;
                while ($set_flag && !feof($fd)) {
                    $line = fgets($fd);
                    if(preg_match ("/^###   @(\d+)=(.*)$/", $line, $matches)) {
                        if (array_key_exists(intval($matches[1]), $hive_keys)) {
                            $hive_item_key = $hive_item_key . "\2" . $matches[2];
                        }
                        $attr = table_column_callback ($db_tbl_str, $db_name, $tbl_name, $matches[2], $matches[1]);
                        $out_line = $out_line . "\1" . $attr;
                    } else {
                        break;
                    }
                }
                $out_line = "$hive_item_key\1" . $out_line;
				list( $part_db_name, $part_tbl_name ) = explode( ".", $db_tbl_str, 2 ) ;
				if( $cfg['db_partitions'] > 1 )
					$out_line = $out_line . "\1" . $part_db_name ;
				else if( $cfg['tbl_partitions'] > 1 )
					$out_line = $out_line . "\1" . $part_tbl_name ;

                $out_line = table_row_callback($db_name, $tbl_name, $out_line, "UPINS");
                if (!empty($out_line)) {
                    file_put_contents ($out_file, $out_line . "\n", FILE_APPEND | LOCK_EX);
                }
            } else if (preg_match ("/^### (DELETE FROM) (${_pattern})$/", $line, $matches)) {
                $set_flag = false;
                $hive_item_key = trim ($matches[2]);
				$db_tbl_str = trim( $matches[2] ) ;
                while (!feof($fd)) {
                    $line = fgets($fd);
                    if(preg_match ("/^### WHERE$/", $line, $matches)) {
                        $set_flag = true;
                        break;
                    }
                    if(!preg_match ("/^### /", $line, $matches)) {
                        break;
                    }
                }
                $out_line = "$unix_time_update\1" . 1;
                while ($set_flag && !feof($fd)) {
                    $line = fgets($fd);
                    if(preg_match ("/^###   @(\d+)=(.*)$/", $line, $matches)) {
                        if (array_key_exists(intval($matches[1]), $hive_keys)) {
                            $hive_item_key = $hive_item_key . "\2" . $matches[2];
                        }
                        $attr = table_column_callback ( $db_tbl_str, $db_name, $tbl_name, $matches[2], $matches[1] ) ;
                        $out_line = $out_line . "\1" . $attr;
                    } else {
                        break;
                    }
                }
                $out_line = "$hive_item_key\1" . $out_line;
                $out_line = table_row_callback($db_name, $tbl_name, $out_line, "DEL");
                if (!empty($out_line)) {
                    file_put_contents ($out_file, $out_line . "\n", FILE_APPEND | LOCK_EX);
                }
            }
            $line = "";
            $line = fgets($fd);
            if (empty($line)) {
                break;
            }
        }

        if(preg_match ("/^#(\d\d\d\d\d\d ( |\d)\d:\d\d:\d\d).*end_log_pos (\d+)/", $line, $matches)) {
            $unix_time_update = str2time($matches[1]).$matches[3];
        }
    }
    fclose($fd);

    write2log("parse $binlog_file finish");
    return $out_file;
}



///////////////////////////////////////////////////////////////////////////
/*
** Usage: Convert Time String in binlog to unixtime
** @Input: $str  --- e.g "121016  9:04:02"
** @Output: int (0 or unix_timestamp)
*/
function str2time ($str) {
    $ftime = strptime($str, "%y%m%d %H:%M:%S");
    $unxTimestamp = mktime(
            $ftime['tm_hour'],
            $ftime['tm_min'],
            $ftime['tm_sec'],
            1 ,
            $ftime['tm_yday'] + 1,
            $ftime['tm_year'] + 1900
        );
    return $unxTimestamp;
}


##########################################################################
### Callback For User Define
##########################################################################
function table_column_callback($dbtbl_prefix, $db_name, $tbl_name, $attr, $pos_in_row) {
    $str = $attr ;
	global $hook_flag ;
    if( $str[0] == "'" )
        $str = substr( $str, 1, strlen($str)-2 ) ;
	if( $hook_flag ){
		$hook_func_callback = "hook_column_{$db_name}_{$tbl_name}" ;
		$str = call_user_func_array( $hook_func_callback, array($dbtbl_prefix, $str, $pos_in_row)) ;
	}

    ## Common Pre-treatment
    $str = str_replace( "\n", "\003", $str ) ;
    $str = str_replace( "\r", "\004", $str ) ;
    $str = str_replace( "\t", "\005", $str ) ;
    $str = str_replace( "\001", "", $str ) ;
	//$str = mb_convert_encoding( $str, "UTF-8", "GBK" ) ;


    if (preg_match("/^(-\d+) \(\d+\)$/", $str, $matches)) {
        //-1 in binlog, maybe is '-1 (65535)' or '-1 (255)'
        $str = $matches[1];
    }
    return $str;
}

function table_row_callback($db_name, $tbl_name, $str, $up_type="HIS") {
    ##TODO: 3 type for source, "UPINS"=>update/insert in binlog, "DEL"=>delete in binlog, "HIS"=> history data in db

    return $str; 
}

?>
