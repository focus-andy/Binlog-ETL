<?php

/*
 * cmds: array of index->parameters to each process
 * logfile: 用于doublecheck是否执行成功的日志文件
 * limit: 每次最多启动多少个进程
 */

function process_parent($cmds, $logfile, $limit=8, $logfunc=parent_default_printer)
{
    $running_cmds = array();
    $running_process = array();
    $running_pipes = array();

    $total_cmd = count($cmds);
    if($total_cmd < $limit)
        $limit = $total_cmd;

    if($limit <= 0)
        $limit = 1;

    while(count($cmds) != 0)
    {
        if(!process_start($cmds,$running_cmds, $running_process, $running_pipes, $limit, $logfile, $logfunc))
            return false;

        $remained = count($cmds);
        $current = count($running_cmds);
        if($remained == 0){
            //最后remained个
            if(!process_monitor($running_cmds, $running_process, $running_pipes, $current, $logfile, $logfunc))
                return false;
        }else{
            //还没启动完，每执行完一个后就启动一个
            if(!process_monitor($running_cmds, $running_process, $running_pipes, 1, $logfile, $logfunc))
                return false;
        }
    }
    return ($failed == 0);
}

/**
 *启动不超过count个进程
 */
function process_start(&$cmds, &$running_cmds, &$running_process, &$running_pipes, $count, $logfile, $logfunc)
{
    $descriptorspec = array(
           0 => array("pipe", "r"), 
           1 => array("pipe", "w"), 
           2 => array("file", $logfile, "a")
    );

    $started = count($running_cmds);
    #$logfunc("need to start $count process\n");
    foreach ($cmds as $key=>$each_cmd) {
        $each_cmd = $each_cmd." 2>&1";
        $process = proc_open($each_cmd, $descriptorspec, $pipes, null, null); 
        if (!$process || !is_resource($process))
        {
            $logfunc("start failed: $each_cmd\n");
            return false;
        } 
        $logfunc("opened $key =>$each_cmd\n");
        $running_pipes[$key] = $pipes;
        $running_process[$key] = $process;
        $running_cmds[$key] = $each_cmd;
        unset($cmds[$key]);
        $started++;
        if($started == $count){
            #$logfunc( "started $started process\n");
            return true;
        }
    }
    return true;
}

/**
 *等待wait_close_count个进程执行结束
 *
 */
function process_monitor(&$running_cmds, &$running_process, &$running_pipes, $wait_close_count, $logfile, $logfunc)
{
    $unfinished = count($running_process);
    if($unfinished < $wait_close_count)
        $wait_close_count = $unfinished;
    $failed = 0;

    while( $wait_close_count > 0)
    {
        sleep(1);
        foreach ($running_cmds as $key=>$each_cmd) {
            $status = proc_get_status($running_process[$key]);
            if(!$status['running'])
            {
                $remained = count($running_cmds);
                $errcode = $status['exitcode'];
                $isfinish = exec("grep \"$key\" $logfile | tail -n 1 | grep Finish");
                if($errcode == 0 || $isfinish != ""){
                    $wait_close_count--;
                    $logfunc("$key finished, remain $wait_close_count\n");
                    unset($running_cmds[$key]);
                    break;
                }else{
                    $wait_close_count--;
                    $failed++;
                    $logfunc("$key error with unknow reason($errcode), remain $wait_close_count\n");
                    unset($running_cmds[$key]);
                    return false; 
                    //break; 
                }
            }
        }
    }
    return true;
}

function parent_default_printer($msg)
{
    echo $msg;
}

?>
