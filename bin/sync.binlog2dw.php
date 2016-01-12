<?php
require ('../conf/sync.conf.php') ;
require_once( '../util/sync.util.php' ) ;
require_once( 'parent.php' ) ;
require_once( 'binlog_parser.php' ) ;
ini_set('memory_limit', '1024M') ;


echo "Usage: ${argv[0]} [tbl=0,1,2...] [type=normal|history|repair|copy] [repair-binlog-start='2012-10-10']\n";
$param_arr = arg_init ($argc, $argv);
if($param_arr === false) {
    exit(1);
}


foreach ($param_arr['table_id'] as $idx) {
    $dt_old = date("Ymd", strtotime("-2 day")); #Default: day before yestoday, e.g 'dt=20121019'
    $dt_new = date("Ymd", strtotime("-1 day")); #Default: yestoday, e.g 'dt=20121020'
    $cfg = $GLOBALS['sync_config']['configs'][$idx];
    if (!isset($cfg['hive_item_key']) || empty($cfg['hive_item_key']) ) {
        write2wflog("Failed: tbl [$idx] have invalid config --- hive_item_key");
        continue;
    }
    $datadir = file_dir_init ($cfg);
    $part = "";
    $partition = "";
    if (isset($cfg['data_sharding_tag'])) {
        $part = "pt={$cfg['data_sharding_tag']}/";
        $partition = ", pt='{$cfg['data_sharding_tag']}'";
    }
    
	$hook_flag = load_hook( $cfg['dbname']. "_". $cfg['tblname'] ) ;

    if ($param_arr['type'] == "history") {
        ##DB table define copy to DW    
        $ret = create_hive_table_if_noexist ($cfg, $datadir['hql_create_tbl_dir']);
        if ($ret == false) {
            write2wflog ("Failed: Create hive table failed, tbl=$idx");
            exit(1);
        } else {
            write2log ("Success: Check & Create hive table");
        }
        ##History data convert to DW
        $outf = convert_dbtable_history ($cfg, $datadir['data_his_dir']);
        if ( 0 != exec("sh hive_tool.sh fput $outf {$cfg['hdfs_snapshot_path']}/dt=history/$part && rm -f $outf")) {
            write2wflog ("Failed: sh hive_tool.sh fput $outf {$cfg['hdfs_snapshot_path']}/dt=history/$part");
            exit(1);
        } else {
            write2log ("Success: put history data to hdfs");
        }
        ##Sync to new snapshot
        wget_binlog_file ($cfg['binlog_remote_addr'], $datadir['binlog_dir'], $dt_old);
        $dt_old = "history";
        $dt_new_0 = $dt_new;
        $dt_new = date("Ymd", strtotime("-1 day"));
        if(!merge_incr_data_from_binlog ($cfg, $idx, $datadir, $dt_new, $dt_old, $dt_new_0)) {
            write2wflog ("Failed: merge failed, parm[$dt_new, $dt_old, $dt_new_0]");
            exit(1);
        } else {
            write2log ("Success: merge success, dt_new=$dt_new");
        }
    } else if ($param_arr['type'] == "normal") {
        ##Check last snapshot exist or not
        exec("sh hive_tool.sh fexist {$cfg['hdfs_snapshot_path']}/dt=$dt_old/$part", $out, $ret);
        if ($ret != 0) {
            write2wflog ("Failed: last snapshot not exist, path[{$cfg['hdfs_snapshot_path']}/dt=$dt_old/$part]");
            exit(1);
        }
        ##Sync to new snapshot
        wget_binlog_file ($cfg['binlog_remote_addr'], $datadir['binlog_dir'], $dt_new);
        write2log ("wget binlog finish ");
        if(!merge_incr_data_from_binlog ($cfg, $idx, $datadir, $dt_new, $dt_old)) {
            write2wflog ("Failed: merge failed, parm[$dt_new, $dt_old]");
        } else {
            write2log ("Success: merge success, dt_new=$dt_new");
        }
    } else if ($param_arr['type'] == "repair") {
        ##Sync to new snapshot
        $binlog_time_start = $param_arr['binlog_start'];
        wget_binlog_file ($cfg['binlog_remote_addr'], $datadir['binlog_dir'], $binlog_time_start);
        for ($dt_beg = $binlog_time_start; $dt_beg <= $dt_new; $dt_beg=date("Ymd", strtotime($dt_beg)+86400)) {
            write2log ("repair data with binlog, dt[$dt_beg] begin...");
            $dt_old_r = date("Ymd", strtotime($dt_beg)-86400);
            $dt_new_r = $dt_beg;
            ##Check last snapshot exist or not
            exec("sh hive_tool.sh fexist {$cfg['hdfs_snapshot_path']}/dt=$dt_old_r/$part", $out, $ret);
            if ($ret != 0) {
                write2wflog ("Failed: last snapshot not exist, path[{$cfg['hdfs_snapshot_path']}/dt=$dt_old_r/$part]");
                exit(1);
            }
            if(!merge_incr_data_from_binlog ($cfg, $idx, $datadir, $dt_new_r, $dt_old_r)) {
                write2wflog ("Failed: merge failed, parm[$dt_new_r, $dt_old_r]");
            } else {
                write2log ("Success: merge success, dt_new=$dt_new_r");
            }
        }
    } else if ($param_arr['type'] == "copy") {
        ##DB table define copy to DW    
        $ret = create_hive_table_if_noexist ($cfg, $datadir['hql_create_tbl_dir'], false);
        if ($ret == false) {
            write2wflog ("Failed: Create hive table failed, tbl=$idx");
            exit(1);
        } else {
            write2log ("Success: Check & Create hive table");
        }
        ##All data in DB copy to DW
        $hdfs_snapshot_new_dir = $cfg['hdfs_snapshot_path'] . "/dt=$dt_new/$part";
        $outf = convert_dbtable_history ($cfg, $datadir['data_his_dir']);
        if ( 0 != exec("sh hive_tool.sh fput $outf $hdfs_snapshot_new_dir && rm -f $outf")) {
            write2wflog ("Failed: sh hive_tool.sh fput $outf $hdfs_snapshot_new_dir");
            exit(1);
        } else {
            write2log ("Success: copy snapshot from DB to hdfs, new dt=$dt_new");
        }
        ##Add partition to hive table
        $hql = "ALTER TABLE {$cfg['hive_snapshot_tblname']} ADD PARTITION (dt='$dt_new'{$partition}) location '${hdfs_snapshot_new_dir}'";
        if (0 != exec("sh hive_tool.sh hql \"$hql\"")) {
            write2wflog ("Failed: add new partition failed, $hql");
        } else {
            write2log ("Success: add new partition dt='$dt_new'{$partition} to {$cfg['hive_snapshot_tblname']}");
        }
    } else {
        exit(1);
    }
    $reserve_days = intval($GLOBALS['sync_config']['local_binlog_reserve_days']);
    if($reserve_days >= 1) {
        clean_binlog_file ($datadir['binlog_dir'], $reserve_days);
    }
}


function table_def_callback($db_name, $tbl_name, $attr_name, $attr_type) {
    ##TODO: some binary type redefined to hive type (e.g ARRAY/MAP), some char reserved in hive
    $_name = $attr_name;
    $_type = $attr_type;

    ## Common Pre-treatment, add prefix to avoid refilct with hive reserved keyword
    $_name = "hk_" . $attr_name;
    $ret = array ('name' => $_name, 'type' => $_type);
    return $ret;
}

##########################################################################
### Convert History Data && Convert binlog increment
##########################################################################

/*
** Usage: Get next N rows from DB
** Input: $db --- db connection
**        $db_config --- db config
**        $latest_gets --- result data set
** Output: return void
*/
function db_get_history_rows ($db, $db_config, &$latest_gets) {
    if (empty($latest_gets)) {
        $latest_gets = array (
            'last_pn' => 0,
            'last_db_idx' => isset($db_config['db_partitions_beg_idx']) ? intval($db_config['db_partitions_beg_idx']) : 0,
            'last_tbl_idx' => isset($db_config['tbl_partitions_beg_idx']) ? intval($db_config['tbl_partitions_beg_idx']) : 0,
            'max_db_idx' => isset($db_config['db_partitions']) ? intval($db_config['db_partitions']) : 1,
            'max_tbl_idx' => isset($db_config['tbl_partitions']) ? intval($db_config['tbl_partitions']) : 1,
            'dbtbl_prefix' => "{$db_config['dbname']}.{$db_config['tblname']}",
			'max_id' => 0,
            'rows' => array(),
        );
    }
    $latest_gets['rows'] = array();
	$primary_key_num =  count( $db_config['hive_item_key'] ) ;


    do {
		//SETP 1 define table name 
        if ( intval($db_config['db_partitions']) > 1 && intval($db_config['tbl_partitions']) > 1 ) {
            $latest_gets['dbtbl_prefix'] = sprintf( "{$db_config['db_tbl_pattern']}",
						$db_config['dbname'], 
						$latest_gets['last_db_idx'], 
						$db_config['tblname'], 
						$latest_gets['last_tbl_idx'] ) ;
        } else if ( intval($db_config['db_partitions']) > 1 ) {
            $latest_gets['dbtbl_prefix'] = sprintf( "{$db_config['db_tbl_pattern']}",
						$db_config['dbname'], 
                        $latest_gets['last_db_idx'],
						$db_config['tblname'] ) ;
        } else if ( intval($db_config['tbl_partitions']) > 1 ) {
            $latest_gets['dbtbl_prefix'] = sprintf( "{$db_config['db_tbl_pattern']}",
						$db_config['dbname'], 
                        $db_config['tblname'],
						$latest_gets['last_tbl_idx'] ) ;
        } else {
            $latest_gets['dbtbl_prefix'] = sprintf( "{$db_config['db_tbl_pattern']}",
						$db_config['dbname'], 
                        $db_config['tblname'] ) ;
        }
		write2log( "STEP 1 TABLE NAME: [{$latest_gets['dbtbl_prefix']}]") ;

		//STEP 2 define a sql
		if( $primary_key_num == 1 ){//single column as primary key
			//get primary key info
			foreach( $db_config['hive_item_key'] as $k => $v ){
				$primary_k = $k ;
				$primary_v = $v ;
			}
			//init primary keys set
			if( $latest_gets['last_pn'] == 0 ) {
				$latest_gets['max_id'] = 0 ;//clean array
				$sql = sprintf ("SELECT max( `%s` ) FROM {$latest_gets['dbtbl_prefix']}", $primary_v ) ;
				$ret = $db->query($sql) ;
				if($ret === false) {
					write2wflog ("Failed: History convert failed: $sql") ;
				}
				while($ret && $tmp = $ret->fetch_row()) {
					$latest_gets['max_id'] = intval( $tmp[0] ) ;
				}
			}

			//sprintf a sql
			if( $latest_gets['last_pn'] <= $latest_gets['max_id'] ) {
				$sql = sprintf( "SELECT * FROM {$latest_gets['dbtbl_prefix']} WHERE `%s` >= %d AND `%s` < %d",
						$primary_v,
						$latest_gets['last_pn'],
						$primary_v,
						$latest_gets['last_pn']+$db_config['max_rows_per_sql']
					) ;
			}

		} else { //moti columns as primary key
			if( $latest_gets['last_pn'] == 0 ) {
				$latest_gets['max_id'] = 0 ;//clean array
				$sql = "SELECT count( * ) FROM {$latest_gets['dbtbl_prefix']}" ;
				$ret = $db->query($sql) ;
				if($ret === false) {
					write2wflog ("Failed: History convert failed: $sql") ;
				}
				while($ret && $tmp = $ret->fetch_row()) {
					$latest_gets['max_id'] = intval( $tmp[0] ) ;
				}
			}

			//sprintf a sql
			$sql = sprintf( "SELECT * FROM {$latest_gets['dbtbl_prefix']} LIMIT %s, %s",
							$latest_gets['last_pn'],
							$db_config['max_rows_per_sql']
						) ;
		}
		write2log( "STEP 2 max_id [{$latest_gets['max_id']}] pn [{$latest_gets['last_pn']}]" ) ;
		write2log( "STEP 2 SQL: [$sql]" ) ;
		//STEP 3 query the sql
		//$db->query( "set names utf8") ;
        $ret = $db->query( $sql ) ;
        if( $ret === false ) {
            write2wflog ("Failed: History convert failed: $sql") ;
        }
        while($ret && $tmp = $ret->fetch_row()) {
            $latest_gets['rows'][] = $tmp ;
        }
		write2log( "STEP 3 ROWS SIZE ". count($latest_gets['rows']) ) ;
		//STEP 4 move pn
        $latest_gets['last_pn'] = intval($latest_gets['last_pn'] + $db_config['max_rows_per_sql']);
		//STEP 5 restart with next partition
		if ( $latest_gets['last_pn'] > $latest_gets['max_id'] ) {
			write2log( "STEP 5 PROCEED") ;
            if ( ( intval($latest_gets['last_db_idx'])+1 < intval($latest_gets['max_db_idx']) )
                || (intval($latest_gets['last_tbl_idx'])+1 < intval($latest_gets['max_tbl_idx'])) ) 
				{
	                if ( intval($latest_gets['last_tbl_idx']) + 1 < intval($latest_gets['max_tbl_idx']) ) {
	                    $latest_gets['last_tbl_idx'] = intval($latest_gets['last_tbl_idx']) + 1 ;
	                } else {
	                    $latest_gets['last_db_idx'] = intval($latest_gets['last_db_idx']) + 1 ;
	                    $latest_gets['last_tbl_idx'] = isset($db_config['tbl_partitions_beg_idx']) 
                                ? intval($db_config['tbl_partitions_beg_idx']) : 0 ;
	                }
	                $latest_gets['last_pn'] = 0 ;
            } else {
                break ;//all partitions are finished
            }
        }
		write2log( "ONE LOOP DONE\n\n") ;
    } while ( empty($latest_gets['rows']) ) ;
}

/*
** Usage: Convert history data from DB to HIVE
** Input: $db_config --- db config
**        $history_out_path --- out data path
** Output: return "the file of out data"
*/ 
function convert_dbtable_history ($db_config, $history_out_path) {
    $db = new mysqli($db_config['db_host'], $db_config['db_user'], $db_config['db_passwd'], $db_config['dbname'], $db_config['db_port']);
    $latest_gets = NULL;
    $uptime = time();
    $out_file = "$history_out_path/{$db_config['dbname']}_{$db_config['tblname']}.history";
    exec ("rm -f $out_file && touch $out_file");

    do {
        db_get_history_rows ($db, $db_config, $latest_gets);
        if(!empty($latest_gets) && !empty($latest_gets['rows'])) {
            foreach ($latest_gets['rows'] as $one_row) {
                $hive_item_key = $latest_gets['dbtbl_prefix'];
                foreach ($db_config['hive_item_key'] as $idk => $idv) {
                    $hive_item_key = $hive_item_key . "\2" . $one_row[intval($idk)-1];
                }
                $out_line = "$hive_item_key\1" . "$uptime\1" . 0;
                for ($i = 0; $i < count($one_row); $i++) {
                    $attr = table_column_callback ( $latest_gets['dbtbl_prefix'], $db_config['dbname'], $db_config['tblname'], $one_row[$i], $i+1);
                    $out_line = $out_line . "\1" . $attr;
                }
				list( $part_db_name, $part_tbl_name ) = explode( ".", $latest_gets['dbtbl_prefix'], 2 ) ;
				if( $db_config['db_partitions'] > 1 )
					$out_line = $out_line . "\1" . $part_db_name ;
				else if( $db_config['tbl_partitions'] > 1 )
					$out_line = $out_line . "\1" . $part_tbl_name ;
                $out_line = table_row_callback($db_config['dbname'], $db_config['tblname'], $out_line);
                file_put_contents ($out_file, $out_line . "\n", FILE_APPEND | LOCK_EX);            
            }
        }
    } while(!empty($latest_gets) && !empty($latest_gets['rows']));

    return $out_file;
}

/*
** Usage: Create hive table
** Input: $db_config --- db config
**        $hive_tbl  --- hive table name
**        $hdfs_file_dir --- hdfs data of hive table
**        $hql_create_file --- hql file for create table
** Output: return true or false
*/
function copy_db_table_define_to_hive ($db_config, $hive_tbl, $hdfs_file_dir, $hql_create_file) {
    $db = new mysqli($db_config['db_host'], $db_config['db_user'], $db_config['db_passwd'], $db_config['dbname'], $db_config['db_port']);

    $last_db_idx = isset($db_config['db_partitions_beg_idx']) ? intval($db_config['db_partitions_beg_idx']) : 0;
    $last_tbl_idx = isset($db_config['tbl_partitions_beg_idx']) ? intval($db_config['tbl_partitions_beg_idx']) : 0;
    if (intval($db_config['db_partitions']) > 1 && intval($db_config['tbl_partitions']) > 1) {
        $dbtbl = sprintf ("{$db_config['db_tbl_pattern']}", $db_config['dbname'], $last_db_idx, $db_config['tblname'], $last_tbl_idx);
    } else if (intval($db_config['db_partitions']) > 1) {
        $dbtbl = sprintf ("{$db_config['db_tbl_pattern']}", $db_config['dbname'], $last_db_idx, $db_config['tblname']);
    } else if (intval($db_config['tbl_partitions']) > 1) {
        $dbtbl = sprintf ("{$db_config['db_tbl_pattern']}", $db_config['dbname'], $db_config['tblname'], $last_tbl_idx);
    } else {
        $dbtbl = sprintf ("{$db_config['db_tbl_pattern']}", $db_config['dbname'], $db_config['tblname']);
    }
    $ret = $db->query("desc {$dbtbl}");
    $gets = array();
    while($ret && $tmp = $ret->fetch_row()) {
        $gets[] = $tmp;
    }
    if(empty($gets)) {
        write2wflog ("Failed: Copy db table define to hive table failed");
        return false;
    }

    $str = "CREATE EXTERNAL TABLE {$hive_tbl} (\n";
    $str = $str . "    hive_item_key STRING, hive_item_version BIGINT, hive_item_valid BIGINT";
    foreach ($gets as $item) {
        $item_name = $item[0];
        if (preg_match("/int/i", $item[1])) {
            $item_type = "BIGINT";
        } else if (preg_match("/(float|double)/i", $item[1])) {
            $item_type = "DOUBLE";
        } else {
            $item_type = "STRING";
        }
        $ret_item = table_def_callback($db_config['dbname'], $db_config['tblname'], $item_name, $item_type);
        if (!empty($ret_item)) {
            $item_type = $ret_item['type'];
            $item_name = $ret_item['name'];
        }
		$str = $str. ",\n	$item_name $item_type" ;
    }
    if ( $db_config['db_partitions'] > 1 || $db_config['tbl_partitions'] > 1 ) {
		$str = $str . ",\n	pt STRING" ;
    } 
    $str = $str . "\n) PARTITIONED BY (dt STRING) ROW FORMAT DELIMITED\n";
    $str = $str . "FIELDS TERMINATED BY '\1' COLLECTION ITEMS TERMINATED BY ','  MAP KEYS TERMINATED BY ':'\n";
    $str = $str . "STORED AS TEXTFILE LOCATION 'hdfs://{$hdfs_file_dir}';\n";
    exec("rm -f $hql_create_file");
    file_put_contents ($hql_create_file, $str . "\n", FILE_APPEND | LOCK_EX);

    return true;
}

/*
** Usage: Get latest binlog file from remote server
** Input: $binlog_online --- online server host and path of binlog
**        $binlog_begin_time --- begin time (e.g. '20121010')
**        $binlog_save_dir --- local save dir of binlog
** Output: return void
*/
function wget_binlog_file ($binlog_online, $binlog_save_dir, $binlog_begin_time, $max_bin_idx = 999999) {
    $idx_count = count($binlog_online);
    for ($idx = 0; $idx < $idx_count; $idx++) {
        $latest_time = strtotime($binlog_begin_time);
        $latest_idx = 1;

        ##local binlog range
        $local_bin_max_idx = 1;
        $local_bin_max_time = 0;
        $local_bin_min_time = 0;
        $dir = scandir($binlog_save_dir, 1);
        foreach ($dir as $fname) {
            if(preg_match ("/mysql-bin\.(\d+)\.(\d+)\.${idx}/", $fname, $res)) {
                //if($local_bin_max_time == 0 || intval($local_bin_max_time) < intval($res[1])) {
                if($local_bin_max_time == 0) {
                    $local_bin_max_idx = intval($res[2]);
                    $local_bin_max_time = $res[1];
                }
                if($local_bin_min_time == 0 || intval($local_bin_min_time) > intval($res[1])) {
                    $local_bin_min_time = $res[1];
                    $local_bin_min_idx = intval($res[2]);
                }
            }
        }
        write2log ("Get binlog from remote-server, local binlog-idx-max [$local_bin_max_idx] " 
            . "binlog-tm [$local_bin_min_time, $local_bin_max_time] need-incr-tm[$binlog_begin_time]");

        if ((intval($local_bin_min_time) > 0) && (intval($local_bin_min_time) < intval($binlog_begin_time))) {
            //已经下载过了
            if ((intval($local_bin_max_time) > 0) && (intval($local_bin_max_time) > intval($binlog_begin_time))) {
                write2log ("All needed binlog have been downloaded");
                return;
            //已存在的binlog过久，从最大的id开始查
            }else if ((intval($local_bin_max_time) > 0) && (intval($local_bin_max_time) < intval($binlog_begin_time))) {
                $latest_idx = $local_bin_max_idx;
            //已存在的binlog有部分当天的，从最小的id开始找
            }else{
                $latest_idx = $local_bin_min_idx;
                //$latest_time = strtotime($local_bin_max_time);
            }
        }
        $curr_idx = $latest_idx;
        $btime = $latest_time;
        $etime = strtotime("now");
        write2log ("Get binlog from remote-server, begin-with-idx[$curr_idx] begin-tm[$btime]");

        while (1) {
            ##scan begin with mysql-bin.$curr_idx
            $binlog_tool = $GLOBALS['sync_config']['mysqlbinlog_path'];
            $unxTimestamp1 = 0;
            $unxTimestamp2 = 0;

            ##binlog file time
            $exe_cmd = sprintf("%s -f -v --base64-output=DECODE-ROWS --stop-position=500 -R %s mysql-bin.%06d "
                    ."| grep -oP '\d\d\d\d\d\d ( |\d)\d:\d\d:\d\d'|head -1", $binlog_tool, $binlog_online[$idx], $curr_idx);
            $out = array();
            exec($exe_cmd, $out, $ret);
            if($ret == 0 && !empty($out[0])) {
                $unxTimestamp1 = str2time($out[0]);
            }
            $exe_cmd = sprintf("%s -f -v --base64-output=DECODE-ROWS --stop-position=500 -R %s mysql-bin.%06d "
                    ."| grep -oP '\d\d\d\d\d\d ( |\d)\d:\d\d:\d\d'|head -1", $binlog_tool, $binlog_online[$idx], 
                    ($curr_idx % $max_bin_idx) + 1);
            $out = array();
            exec($exe_cmd, $out, $ret);
            if($ret == 0 && !empty($out[0])) {
                $unxTimestamp2 = str2time($out[0]);
            }

            ##spilt binlog file with time suffix
            if ($unxTimestamp1 != 0 && ($unxTimestamp2 == 0 or $unxTimestamp2 > $btime)) {
                $curr_unixtime =  strtotime(date("Y-m-d", $unxTimestamp1));
                $curr_end = ($unxTimestamp2 == 0) ? $etime : strtotime(date("Y-m-d", $unxTimestamp2+86400));
                while (1) {
                    ##split binlog file to daily-local-file
                    $start_datetime = date("Y-m-d H:i:s", $curr_unixtime);
                    $stop_datetime = date("Y-m-d H:i:s", $curr_unixtime + 86400);
                    $local_binlog_file = sprintf("%s/mysql-bin.%s.%06d.%d", $binlog_save_dir, 
                            date("Ymd", $curr_unixtime), $curr_idx, $idx);
                    //if(!file_exists($local_binlog_file)) { //会出现下载当天只下载一半,第二天没有继续的情况,暂时注释掉
                        $binlog_tmp = sprintf("%s/tmp.mysql-bin.%s.%06d.%d.%d", $binlog_save_dir,
                            date("Ymd", $curr_unixtime), $curr_idx, $idx, time());
                        $exe_cmd = sprintf("%s -f -v --base64-output=DECODE-ROWS --start-datetime='%s' "
                            . " --stop-datetime='%s' -R %s mysql-bin.%06d > %s", $binlog_tool, $start_datetime,
                            $stop_datetime, $binlog_online[$idx], $curr_idx, $binlog_tmp);
                        $out = array();
                        write2log ("Start get binlog $binlog_tmp");
                        exec($exe_cmd, $out, $ret);
                        exec("mv $binlog_tmp $local_binlog_file");
                        write2log ("Finish get binlog $binlog_tmp");
                    //}else {
                    //    write2log ("$local_binlog_file is already downloaded");
                    //}
                    $curr_unixtime += 86400;
                    if($curr_unixtime >= $curr_end) {
                        break;
                    }
                }
            }
            $curr_idx = ($curr_idx % $max_bin_idx) + 1;
            if($curr_idx == $latest_idx || ($unxTimestamp1 != 0 && $unxTimestamp2 == 0)) {
                break;
            }
        }
    }
}

/*
** Usage: Clean up old binlog file
** Input: $binlog_dir --- binlog file
**        $reserve_days --- binlog reserve days
** Output: return void
*/
function clean_binlog_file ($binlog_dir, $reserve_days) {
    $tm = strtotime(date("Y-m-d", strtotime("-$reserve_days day")));
    $dir = scandir($binlog_dir, 1);

    foreach ($dir as $fname) {
        if(preg_match ("/mysql-bin.(\d+).(\d+)/", $fname, $res)) {
            if(strtotime($res[1]) < $tm) {
                exec("rm -f $binlog_dir/$fname");
            }
        }
    }
}


/*
** Usage: Write to log file
** @Input: $str
** @Output: return void
*/
function write2log ($str) {
    global $datadir;
    $log_fd = $datadir['logs_dir'] . "/exec.log." . date("Ymd", strtotime("now"));
    $logstr = "NOTICE: " . date('Y-m-d H:i:s') . " $str\n";
    file_put_contents ($log_fd, $logstr, FILE_APPEND | LOCK_EX);
}
/*
** Usage: Write to log file
** @Input: $str
** @Output: return void
*/
function write2wflog ($str) {
    global $datadir;
    $wflog_fd = $datadir['logs_dir'] . "/exec.log." . date("Ymd", strtotime("now")).".wf";
    $logstr = "WARNING: " . date('Y-m-d H:i:s') . " $str\n";
    file_put_contents ($wflog_fd, $logstr, FILE_APPEND | LOCK_EX);
}

/*
** Usage: Read Param
** @Input: $argc, $argv
** @Output: return array
*/
function arg_init ($argc, $argv) {
    $retv = array(
        'type' => '',
        'table_id' => array(),
        'binlog_start' => '',
    );

    if ($argc < 3) {
        echo "Param error, need 'tbl=XXX' and 'type=XXX'\n";
        return false;
    }
    
    for ($i=1; $i< $argc; $i++) {
        $a = explode("=", $argv[$i], 2);
        if (isset($a[0]) && $a[0]=="tbl") {
            $retv['table_id'] = explode(",", $a[1]);
        } else if (isset($a[0]) && $a[0]=="type") {
            $retv['type'] = $a[1];
        } else if (isset($a[0]) && $a[0]=="repair-binlog-start") {
            $retv['binlog_start'] = date("Ymd", strtotime($a[1]));
        } else {
            echo "argv " . $argv[$i] . " is INVALID param\n";
            return false;
        }
    }
    
    if (empty($retv['type']) || empty($retv['table_id'])) {
        echo "Param error, need 'tbl=XXX' and 'type=XXX'\n";
        return false;
    }
    if ($retv['type'] != "normal" && $retv['type'] != "history" && $retv['type'] != "repair" && $retv['type'] != "copy") {
        echo "Param error, arg 'type'=${retv['type']} is INVALID\n";
        return false;
    }
    if ($retv['type'] == "repair" && empty($retv['binlog_start'])) {
        echo "Param error, while type='repair', arg 'repair-binlog-start' is needed\n";
        return false;
    }
    foreach ($retv['table_id'] as $idx) {
        if (!isset( $GLOBALS['sync_config']['configs'][intval($idx)] )) {
            echo "Param error, arg 'tbl' is INVALID, tbl=$idx no exist in config\n";
            return false;
        }
    }
    
    return $retv;
}

/*
** Usage: Create data out dir
** @Input: $cfg     --- table config (with: 'dbname', 'tblname', etc)
** @Output: return array
*/
function file_dir_init ($cfg) {
    $sub_path="";
    if (isset($cfg['data_sharding_tag'])) {
        $sub_path = "{$cfg['data_sharding_tag']}/";
    }
    $binlog_dir = $GLOBALS['sync_config']['local_data_save_dir'] . "/{$cfg['dbname']}_{$cfg['tblname']}/{$sub_path}binlog/";
    $data_his_dir = $GLOBALS['sync_config']['local_data_save_dir'] . "/{$cfg['dbname']}_{$cfg['tblname']}/{$sub_path}history/";
    $data_daily_dir = $GLOBALS['sync_config']['local_data_save_dir'] . "/{$cfg['dbname']}_{$cfg['tblname']}/{$sub_path}daily/";
    $hql_create_tbl_dir = $GLOBALS['sync_config']['local_data_save_dir'] . "/{$cfg['dbname']}_{$cfg['tblname']}/{$sub_path}hivetbl/";
    $logs_dir = $GLOBALS['sync_config']['local_data_save_dir'] . "/{$cfg['dbname']}_{$cfg['tblname']}/{$sub_path}logs/";
    if ( isset($cfg['binlog_local_save_path']) ) {
        $binlog_dir = $cfg['binlog_local_save_path'] . "/{$sub_path}";
    }
    if (!is_dir($binlog_dir)) {
        @mkdir($binlog_dir, 0777, true);
    }
    if (!is_dir($data_his_dir)) {
        @mkdir($data_his_dir, 0777, true);
    }
    if (!is_dir($data_daily_dir)) {
        @mkdir($data_daily_dir, 0777, true);
    }
    if (!is_dir($hql_create_tbl_dir)) {
        @mkdir($hql_create_tbl_dir, 0777, true);
    }
    if (!is_dir($logs_dir)) {
        @mkdir($logs_dir, 0777, true);
    }
    
    return array(
        'logs_dir' => $logs_dir,
        'binlog_dir' => $binlog_dir,
        'data_his_dir' => $data_his_dir,
        'data_daily_dir' => $data_daily_dir,
        'hql_create_tbl_dir' => $hql_create_tbl_dir,
    );
}



/*
** Usage: Create hive table if not exist
** @Input: $cfg     --- table config (with: 'dbname', 'tblname', etc)
** @Input: $hql_create_tbl_dir
** @Output: return true or false
*/
function create_hive_table_if_noexist ($cfg, $hql_create_tbl_dir, $daily_create = true) {
    exec("sh hive_tool.sh hql \"desc {$cfg['hive_snapshot_tblname']}\"", $out, $ret);
    if ($ret != 0) {
        $hql_file_snapshot = $hql_create_tbl_dir."/{$cfg['hive_snapshot_tblname']}.hql";
        if( !copy_db_table_define_to_hive($cfg, $cfg['hive_snapshot_tblname'], $cfg['hdfs_snapshot_path'],
            $hql_file_snapshot) ) {
            write2wflog("Failed: Cannot create table {$cfg['hive_snapshot_tblname']}");
            return false;
        }
        if(0 != exec("sh hive_tool.sh fhql $hql_file_snapshot")) {
            write2wflog("Failed: Cannot create table {$cfg['hive_snapshot_tblname']}, exec failed: sh hive_tool.sh fhql $hql_file_snapshot");
            return false;
        }
        if ($daily_create) {
            $hql_file_daily = $hql_create_tbl_dir."/{$cfg['hive_daily_increment_tblname']}.hql";
            if( !copy_db_table_define_to_hive($cfg, $cfg['hive_daily_increment_tblname'], $cfg['hdfs_daily_increment_path'],
                $hql_file_daily) ) {
                write2wflog("Failed: Cannot create table {$cfg['hive_daily_increment_tblname']}");
                return false;
            }
            if(0 != exec("sh hive_tool.sh fhql $hql_file_daily")) {
                write2wflog("Failed: Cannot create table {$cfg['hive_daily_increment_tblname']}, exec failed: sh hive_tool.sh fhql $hql_file_daily");
                return false;
            }
        }
    }
    return true;
}

/*
** @Input: $cfg      --- table config (with: 'dbname', 'tblname', etc)
** @Input: $idx      --- idx of table
** @Input: $datadir  --- data dir (with: 'binlog_dir', 'data_daily_dir', etc.)
** @Input: $dt_new   --- new snapshot (e.g. '20121011')
** @Input: $dt_old   --- old snapshot (e.g. '20121010' or 'history')
** @Input: $dt_new_0 --- daily point begin (e.g. 'history' + '20121007~20121011'daily => '20121011'snap)
** @Input: 
** @Output: return true or false
*/
function parse_incr_data_from_binlog ($cfg, $idx, $datadir, $dt_new, $dt_new_0, $part, $partition) {
    $hdfs_daily_dir = $cfg['hdfs_daily_increment_path'] . "/dt=$dt_new/$part";
    $log_fd = $datadir['logs_dir'] . "/exec.log." . date("Ymd", strtotime("now"));

    $dir = scandir($datadir['binlog_dir']);
    if (empty($dt_new_0) || $dt_new_0 > $dt_new) { $dt_new_0 = $dt_new; }
    $soncmd = get_php_path()." put_incr_data.php ";
    $cmds = array();
    $bindir = $datadir['binlog_dir'];
    write2log ("date from $dt_new_0 - $dt_new, dir is $bindir ");
    $no = 1;
    for ($dt_inc = $dt_new_0; $dt_inc <= $dt_new; $dt_inc=date("Ymd", strtotime($dt_inc)+86400)) {
        foreach ($dir as $fname) {
            if(preg_match ("/^mysql-bin\.$dt_inc\.\d+\.\d+/", $fname, $res)) {
                write2log ("$fname need to parse...");
                $outf = "{$datadir['data_daily_dir']}/{$cfg['dbname']}.{$cfg['tblname']}.$dt_new.$no";
                //exec ("rm -f $outf && touch $outf");
                $cmd = "$soncmd $idx {$datadir['binlog_dir']}/$fname $outf $hdfs_daily_dir $log_fd";
                $cmds[$fname] = $cmd;
                $no++;
            }
        }
    }

    $max_concurrent_cnt = 128;
    if (isset($GLOBALS['sync_config']['concurrent_count'])) {
        $max_concurrent_cnt = intval($GLOBALS['sync_config']['concurrent_count']) ;
    }
    write2log ("max $max_concurrent_cnt processes will be concurrent");
    return process_parent($cmds, $log_fd, $max_concurrent_cnt, write2log);
}
 
/*
** @Input: $cfg      --- table config (with: 'dbname', 'tblname', etc)
** @Input: $idx      --- idx of table
** @Input: $datadir  --- data dir (with: 'binlog_dir', 'data_daily_dir', etc.)
** @Input: $dt_new   --- new snapshot (e.g. '20121011')
** @Input: $dt_old   --- old snapshot (e.g. '20121010' or 'history')
** @Input: $dt_new_0 --- daily point begin (e.g. 'history' + '20121007~20121011'daily => '20121011'snap)
** @Input: 
** @Output: return true or false
*/
function merge_incr_data_from_binlog ($cfg, $idx, $datadir, $dt_new, $dt_old, $dt_new_0="") {
    $part = "";
    $partition = "";
    if (isset($cfg['data_sharding_tag'])) {
        $part = "pt={$cfg['data_sharding_tag']}/";
        $partition = ", pt='{$cfg['data_sharding_tag']}'";
    }

    if(!parse_incr_data_from_binlog ($cfg, $idx, $datadir, $dt_new, $dt_new_0, $part, $partition)){
        write2log ("parse binlog of $idx in $datadir failed");
        return false;
    }

    ##MERGE: daily + old ==> new
    $hdfs_daily_dir = $cfg['hdfs_daily_increment_path'] . "/dt=$dt_new/$part";
    $hdfs_snapshot_old_dir = $cfg['hdfs_snapshot_path'] . "/dt=$dt_old/$part";
    $hdfs_snapshot_new_dir = $cfg['hdfs_snapshot_path'] . "/dt=$dt_new/$part";
    $hdfs_php_env_path = $GLOBALS['sync_config']['hdfs_base_dir'] . "/home_php.tgz";
    write2log("hdfs_daily_dir is $hdfs_daily_dir\thdfs_snapshot_old_dir is $hdfs_snapshot_old_dir\thdfs_snapshot_new_dir is $hdfs_snapshot_new_dir") ;

    exec("sh hive_tool.sh fexist $hdfs_php_env_path", $out, $ret);
    if ($ret != 0) {
        if(0 != exec("sh hive_tool.sh fput home_php.tgz $hdfs_daily_dir")) {
            write2wflog ("Failed: sh hive_tool.sh fput home_php.tgz $hdfs_daily_dir");
            return false;
        }
    }
    if (0 != exec("sh hive_tool.sh fmerge $hdfs_daily_dir $hdfs_snapshot_old_dir $hdfs_snapshot_new_dir $hdfs_php_env_path")) {
        write2wflog ("Failed: sh hive_tool.sh fmerge $hdfs_daily_dir $hdfs_snapshot_old_dir $hdfs_snapshot_new_dir");
        return false;
    } else {
        write2log ("Success: merge finish, new snapshot: $hdfs_snapshot_new_dir");
    }

    ##HIVE-PARTITION: add new snapshot to hive table partition
    $hql = "ALTER TABLE {$cfg['hive_snapshot_tblname']} ADD PARTITION (dt='$dt_new'{$partition}) location '${hdfs_snapshot_new_dir}'";
    if (0 != exec("sh hive_tool.sh hql \"$hql\"")) {
        write2wflog ("Failed: add new partition failed, $hql");
    } else {
        write2log ("Success: add new partition dt='$dt_new'{$partition} to {$cfg['hive_snapshot_tblname']}");
    }
    $hql = "ALTER TABLE {$cfg['hive_daily_increment_tblname']} ADD PARTITION (dt='$dt_new'{$partition}) location '${hdfs_daily_dir}'";
    if (0 != exec("sh hive_tool.sh hql \"$hql\"")) {
        write2wflog ("Failed: add new partition failed, $hql");
    } else {
        write2log ("Success: add new partition dt='$dt_new'{$partition} to {$cfg['hive_daily_increment_tblname']}");
    }

    return true;
}

function get_php_path()
{
    global $argv;
    $cmd = "ps ax | grep $argv[0] | grep -v \"grep $argv[0]\" | grep -v \"vim\" | awk '{print $5}'    ";
    $path = exec($cmd);
    if(!strstr($path, 'php'))
        $path='php';
    return $path;
}


?>
