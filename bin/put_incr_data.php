<?php
require ('../conf/sync.conf.php') ;
require_once("binlog_parser.php");

if( $argc != 6 )
{
    echo "Error: error argc number $argc";
    exit(-1);
}
$cfg = $GLOBALS['sync_config']['configs'][$argv[1]];
$logfile = $argv[5];
$binfile = $argv[2];
$hook_flag = load_hook( $cfg['dbname']. "_". $cfg['tblname'] ) ;
    

if(put_incr_data_from_binlog($cfg, $argv[2], $argv[3], $argv[4])){
    write2log("Finished");
    exit(0);
}else{
    write2log("Error: parse failed");
    exit(-1);
}

/*
** Usage: Write to log file
** @Input: $str
** @Output: return void
*/
function write2log ($str) {
    global $logfile;
    global $binfile;
    $log_fd = $logfile;
    $logstr = "NOTICE: " . date('Y-m-d H:i:s') . "$binfile :: $str\n";
    file_put_contents ($log_fd, $logstr, FILE_APPEND | LOCK_EX);
}
/*
** Usage: Write to log file
** @Input: $str
** @Output: return void
*/
function write2wflog ($str) {
    global $logfile;
    global $binfile;
    $wflog_fd = $logfile."wf";
    $logstr = "WARNING: " . date('Y-m-d H:i:s') . "$binfile :: $str\n";
    file_put_contents ($wflog_fd, $logstr, FILE_APPEND | LOCK_EX);
}

/*
** @Input: $cfg      --- table config (with: 'dbname', 'tblname', etc)
** @Input: $datadir  --- data dir (with: 'binlog_dir', 'data_daily_dir', etc.)
** @Input: $outf --- local temp data output file
** @Input: $hdfs_daily_dir  --- hdfs put dir
** @Input: 
** @Output: return true or false
*/
function put_incr_data_from_binlog ($cfg, $datafile, $outf, $hdfs_daily_dir) {
    $time_s = exec("date");
    #write2log("$time_s: start parse");
    exec ("rm -f $outf && touch $outf");
    parse_binlog_rows($datafile, $cfg, $outf);
    $time_e = exec("date");
    write2log("finish parse($time_s  -- $time_e)");

    if(0 != exec("sh hive_tool.sh fput $outf $hdfs_daily_dir")) {
        write2wflog ("Failed: sh hive_tool.sh fput $outf $hdfs_daily_dir");
        return false;
    } else {
        write2log ("Success: convert daily inc finish, new daily: $hdfs_daily_dir");
    }

    return true;
}

?>
