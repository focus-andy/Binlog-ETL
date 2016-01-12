<?php

$GLOBALS['sync_config'] = array(
    'local_data_save_dir' => dirname(dirname(__FILE__)) . "/data/",
    'mysqlbinlog_path' => '/home/iknow/mysql51/bin/mysqlbinlog',
    'local_binlog_reserve_days' => 3,
    'hdfs_base_dir' => '/app/ns/iknow/',
    'concurrent_count' => 4,
    'configs' => array (
        ######################################################
        ## Table 0 && 1 (ͬһ??mysql??Ⱥ?ı?ͬ??????binlog??־??case)
        ## ---binlog_local_save_path
        ######################################################
        array ( //Table 0
            ## primary key in DB ($1, $2, $3 ...), mapping to hive key
			'hive_item_key' => array (1 => 'id'),
            'max_rows_per_sql' => 10000,
            'binlog_local_save_path' => dirname(dirname(__FILE__)) . '/data/act_binlog/',
            ## config for sync history data in DB
            'db_host' => '10.38.40.13',
            'db_port' => 5400,
            'db_user' => 'binlog_r',
            'db_passwd' => 'XXXXXX',
            ## config for sync binlog
            'binlog_remote_addr' => array (
                '-h10.38.40.13 -P5400 -ubinlog_r -pXXXXXX',
            ),
            ## table info
            'dbname' => 'uteam',
            'tblname' => 'teamInfo',
            'db_tbl_pattern' => '%s.%s',
            'db_partitions' => 1,
            'db_partitions_beg_idx' => '0', //Default begin with 0, valid while db_partitions > 1
            'tbl_partitions' => 1,
            'tbl_partitions_beg_idx' => '0', //Default begin with 0, valid while db_partitions > 1
            ## config for sync data to HIVE
            'hdfs_snapshot_path' => '/app/ns/iknow/hive/warehouse/uteam/teamInfo/',
            'hdfs_daily_increment_path' => '/app/ns/iknow/hive/warehouse/uteam/teamInfo_dayinc/',
            'hive_snapshot_tblname' => 'uteam_teamInfo',
            'hive_daily_increment_tblname' => 'uteam_teamInfo_dayinc',
        ),
        array ( //Table 1
            ## primary key in DB ($1, $2, $3 ...), mapping to hive key
			'hive_item_key' => array (	1 => 'id1', 
										2 => 'id2', 
										3 => 'id3' ),
            'max_rows_per_sql' => 10000,
            'binlog_local_save_path' => dirname(dirname(__FILE__)) . '/data/binlog_act/',
            ## config for sync history data in DB
            'db_host' => '10.38.40.13',
            'db_port' => 5400,
            'db_user' => 'binlog_r',
            'db_passwd' => 'XXXXXX',
            ## config for sync binlog
            'binlog_remote_addr' => array (
                '-h10.38.40.13 -P5400 -ubinlog_r -pXXXXXX',
            ),
            ## table info
            'dbname' => 'uteam',
            'tblname' => 'teamAnswer',
            'db_tbl_pattern' => '%s.%s',
            'db_partitions' => 1,
            'db_partitions_beg_idx' => '0', //Default begin with 0, valid while db_partitions > 1
            'tbl_partitions' => 1,
            'tbl_partitions_beg_idx' => '0', //Default begin with 0, valid while db_partitions > 1
            ## config for sync data to HIVE
            'hdfs_snapshot_path' => '/app/ns/iknow/hive/warehouse/uteam/teamAnswer/',
            'hdfs_daily_increment_path' => '/app/ns/iknow/hive/warehouse/uteam/teamAnswer_dayinc/',
            'hive_snapshot_tblname' => 'uteam_teamAnswer',
            'hive_daily_increment_tblname' => 'uteam_teamAnswer_dayinc',
        ),
        ######################################################
        ## Table 2 (?ֱ???case???ֿ???ͬ)
        ## --- db_tbl_pattern
        ######################################################
        array ( //Table 2
            ## primary key in DB ($1, $2, $3 ...), mapping to hive key
			'hive_item_key' => array (1 => 'id'),
            'max_rows_per_sql' => 10000,
            'binlog_local_save_path' => dirname(dirname(__FILE__)) . '/data/rank_binlog/',
            ## config for sync history data in DB
            'db_host' => '10.26.17.8',
            'db_port' => 4300,
            'db_user' => 'ns_iknow_all_r',
            'db_passwd' => 'XXXXXX',
            ## config for sync binlog
            'binlog_remote_addr' => array (
                '-h10.36.125.56 -P5300 -ubinlog_r -pXXXXXX ',
            ),
            ## table info
            'dbname' => 'ns_iknow_ustatis',
            'tblname' => 'tblScoreCid',
            'db_tbl_pattern' => '%s.%s_%d',
            'db_partitions' => 1,
            'db_partitions_beg_idx' => '0', //Default begin with 0, valid while db_partitions > 1
            'tbl_partitions' => 50,
            'tbl_partitions_beg_idx' => '0', //Default begin with 0, valid while db_partitions > 1
            ## config for sync data to HIVE
            'hdfs_snapshot_path' => '/app/ns/iknow/hive/warehouse/ns_iknow_ustatis/tblScoreCid/',
            'hdfs_daily_increment_path' => '/app/ns/iknow/hive/warehouse/ns_iknow_ustatis/tblScoreCid_dayinc/',
            'hive_snapshot_tblname' => 'tblScoreCid',
            'hive_daily_increment_tblname' => 'tblScoreCid_dayinc',
        ),
        ######################################################
        ## Table 3 (??key��????????case)
        ## --- hive_item_key
        ######################################################
        array ( //Table 3
            ## primary key in DB ($1, $2, $3 ...), mapping to hive key
			'hive_item_key' => array (	1 => 'id1',
										2 => 'id2' ),
            'max_rows_per_sql' => 10000,
            'binlog_local_save_path' => dirname(dirname(__FILE__)) . '/data/binlog_act/',
            ## config for sync history data in DB
            'db_host' => '10.38.40.13',
            'db_port' => 5400,
            'db_user' => 'binlog_r',
            'db_passwd' => 'XXXX',
            ## config for sync binlog
            'binlog_remote_addr' => array (
                '-h10.38.40.13 -P5400 -ubinlog_r -pXXXX',
            ),
            ## table info
            'dbname' => 'uoperator',
            'tblname' => 'uoperator_info',
            'db_tbl_pattern' => '%s.%s',
            'db_partitions' => 1,
            'db_partitions_beg_idx' => '0', //Default begin with 0, valid while db_partitions > 1
            'tbl_partitions' => 1,
            'tbl_partitions_beg_idx' => '0', //Default begin with 0, valid while db_partitions > 1
            ## config for sync data to HIVE
            'hdfs_snapshot_path' => '/app/ns/iknow/hive/warehouse/uoperator/uoperator_info/',
            'hdfs_daily_increment_path' => '/app/ns/iknow/hive/warehouse/uoperator/uoperator_info_dayinc/',
            'hive_snapshot_tblname' => 'uoperator_uoperator_info',
            'hive_daily_increment_tblname' => 'uoperator_uoperator_info_dayinc',
        ),
        ###################################################################
        ## Table 4 (mysql?????л?ʱ?ļ???case, ?л???????binlog??־??ɢ?ڶ?̨mysql)
        ## --- binlog_remote_addr
        ###################################################################
        array ( //Table 4
            ## primary key in DB ($1, $2, $3 ...), mapping to hive key
			'hive_item_key' => array (	1 => 'id1',
										2 => 'id2',
										3 => 'id3',
										4 => 'id4' ),
            'max_rows_per_sql' => 10000,
            'binlog_local_save_path' => dirname(dirname(__FILE__)) . '/data/binlog_act/',
            ## config for sync history data in DB
            'db_host' => '10.38.40.13',
            'db_port' => 5400,
            'db_user' => 'binlog_r',
            'db_passwd' => 'XXX',
            ## config for sync binlog
            'binlog_remote_addr' => array (
                '-h10.38.40.13 -P5400 -ubinlog_r -pXXX', //??ͬ??Դ
                '-hXXXXXXXXXXX -P5400 -ubinlog_r -pXXX', //??ͬ??Դ
            ),
            ## table info
            'dbname' => 'iask',
            'tblname' => 'AuidRelation',
            'db_tbl_pattern' => '%s.%s',
            'db_partitions' => 1,
            'db_partitions_beg_idx' => '0', //Default begin with 0, valid while db_partitions > 1
            'tbl_partitions' => 1,
            'tbl_partitions_beg_idx' => '0', //Default begin with 0, valid while db_partitions > 1
            ## config for sync data to HIVE
            'hdfs_snapshot_path' => '/app/ns/iknow/hive/warehouse/iask/AuidRelation/',
            'hdfs_daily_increment_path' => '/app/ns/iknow/hive/warehouse/iask/AuidRelation_dayinc/',
            'hive_snapshot_tblname' => 'iask_AuidRelation',
            'hive_daily_increment_tblname' => 'iask_AuidRelation_dayinc',
        ),
        ###################################################################
        ## Table 5 (??ʷ???յ???ʱ??order by????????)
        ## --- tbl_rows_orderby : ??ʷ???յ???ʱselect ... order by????
        ###################################################################
        array ( //Table 5
            ## primary key in DB ($1, $2, $3 ...), mapping to hive key
			'hive_item_key' => array (	1 => 'id1',
										2 => 'id2'),
            'max_rows_per_sql' => 10000,
            'binlog_local_save_path' => dirname(dirname(__FILE__)) . '/data/binlog_act/',
            ## config for sync history data in DB
            'db_host' => '10.38.40.13',
            'db_port' => 5400,
            'db_user' => 'binlog_r',
            'db_passwd' => 'XXX',
            ## config for sync binlog
            'binlog_remote_addr' => array (
                '-h10.38.40.13 -P5400 -ubinlog_r -pXXX',
            ),
            ## table info
            'dbname' => 'iask',
            'tblname' => 'AuidRelation',
            'db_tbl_pattern' => '%s.%s',
            'db_partitions' => 1,
            'db_partitions_beg_idx' => '0', //Default begin with 0, valid while db_partitions > 1
            'tbl_partitions' => 1,
            'tbl_partitions_beg_idx' => '0', //Default begin with 0, valid while db_partitions > 1
            'tbl_rows_orderby' => 'order by uid asc, rid asc',
            ## config for sync data to HIVE
            'hdfs_snapshot_path' => '/app/ns/iknow/hive/warehouse/iask/AuidRelation/',
            'hdfs_daily_increment_path' => '/app/ns/iknow/hive/warehouse/iask/AuidRelation_dayinc/',
            'hive_snapshot_tblname' => 'iask_AuidRelation',
            'hive_daily_increment_tblname' => 'iask_AuidRelation_dayinc',
        ),

    ),
);


?>
