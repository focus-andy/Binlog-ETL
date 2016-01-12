#!/bin/sh

env_init() {
    ## make sure the HADOOP_HOME/HIVE_HOME/JAVA_HOME is ok
    cur_dir=`pwd`
    cd
    source .bashrc
    cd $cur_dir
}
hdfs_put() {
    # $1 --- local source file
    # $2 --- hdfs dir
    ret=`${HADOOP_HOME}/bin/hadoop fs -ls $2`
    if [ $? != 0 ] 
    then
        ${HADOOP_HOME}/bin/hadoop fs -mkdir $2
    fi
    fname=`basename $1`
    ret=`${HADOOP_HOME}/bin/hadoop fs -ls $2/$fname`
    if [ $? == 0 ]
    then
        newpath=$2/$fname.bak
        ${HADOOP_HOME}/bin/hadoop fs -mv $2/$fname $newpath
    fi
    ${HADOOP_HOME}/bin/hadoop fs -put $1 $2
}
hdfs_file_exist() {
    # $1 --- hdfs_file or hdfs_dir
    #ret=`${HADOOP_HOME}/bin/hadoop fs -ls $1`
    echo "${HADOOP_HOME}/bin/hadoop fs -ls $1"
    if [ $? != 0 ]
    then
        exit 1
    fi
    exit 0
}
hdfs_file_merge() {
    # $1 --- hdfs_daily_increment_path
    # $2 --- hdfs_snapshot_path (old)
    # $3 --- hdfs_snapshot_path (new)
    # $4 --- hdfs_php_env_path
    ret=`${HADOOP_HOME}/bin/hadoop fs -ls $3`
    if [ $? == 0 ]
    then
        newpath=${3/%\//}.bak
        ${HADOOP_HOME}/bin/hadoop fs -mv $3 $newpath
    fi
    mr_arg="-file merge_mapper.php -file merge_reducer.php -cacheArchive ${4}#homephp"
    ${HADOOP_HOME}/bin/hadoop streaming -D mapred.textoutputformat.ignoreseparator=true -input $1 -input $2 -output $3 -mapper ./merge_mapper.php -reducer ./merge_reducer.php $mr_arg
}
hive_exec_hql() {
    $HIVE_HOME/bin/hive -e "$1"
}
hive_exec_hql_file() {
    $HIVE_HOME/bin/hive -f $1
}

env_init

case C"$1" in
    Cfput)
        hdfs_put $2 $3
        exit $?
        ;;
    Cfexist)
        hdfs_file_exist $2
        exit $?
        ;;
    Cfmerge)
        hdfs_file_merge $2 $3 $4 $5
        exit $?
        ;;
    Chql)
        hive_exec_hql "$2"
        exit $?
        ;;
    Cfhql)
        hive_exec_hql_file $2
        exit $?
        ;;
    C*)
        echo "Usage: $0 {fput|fexist|fmerge|hql|fhql}"
        ;;
esac
