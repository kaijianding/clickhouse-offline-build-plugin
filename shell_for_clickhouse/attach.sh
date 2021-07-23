#!/bin/bash

cd $(dirname $0)/..

export DEFAULT_CLICKHOUSE_USER='default'
export DEFAULT_CLICKHOUSE_PASSWD='default_password'
host='localhost:8023'

usage="Usage: attach.sh database table hdfs_part_path"

if [ $# -lt 3 ]; then
  echo $usage
  exit 1
fi
database=$1
table=$2
hdfs_part_path=$3

function pickDisk() {
    disks=`echo "select data_paths from system.tables where database='$database' and name='$table'" | curl -s "http://${DEFAULT_CLICKHOUSE_USER}:${DEFAULT_CLICKHOUSE_PASSWD}@${host}/?database=${database}" -d @-`
    server_ok=$?
    if [ "$server_ok" != "0" ]; then
        echo "$host server is down?"
        exit 1
    fi
    if [ "$disks" == "" ]; then
        echo "$database.$table not exist?"
        exit 1
    fi
    if [[ "$disks" =~ "Exception" ]]; then
        echo "$disks"
        exit 1
    fi
    disks=`echo $disks|sed "s/'//g"|sed 's/\[//g'|sed 's/]//g'`
    arr=(${disks//,/ })
    disks_length=`echo ${#arr[@]}`
    picked_disk_index=`echo $((RANDOM %$disks_length))`
    echo ${arr[picked_disk_index]}
}

function downloadAndUnzip() {
    disk=$1
    hdfs_part_path=$2
    arr=(${hdfs_part_path//\// })
    part_length=`echo ${#arr[@]}`
    hdfs_file_name=${arr[part_length-1]}
    part_zip_name=(${hdfs_file_name//\_\_/ })
    part_zip_name=`echo ${part_zip_name[1]}`
    target_zip_path="$disk"detached/"$part_zip_name"
    rm -f $target_zip_path
    `which hdfs` dfs -get hdfs_part_path > $target_zip_path
    if [ "$?" != "0" ]; then
        echo "download $hdfs_part_path failed or target disk $disk is bad disk, always try clean"
        rm -f $target_zip_path
        exit 1
    fi
    part_name=`echo $part_zip_name|sed "s/.zip//g"`
    target_path=`echo $target_zip_path|sed "s/.zip//g"`
    rm -fr $target_path
    unzip -q $target_zip_path -d $target_path
    if [ "$?" != "0" ]; then
        echo "unzip $target_zip_path failed, $part_zip_name is corrupted or target disk $disk is bad disk, always try clean"
        rm -fr $target_path
        rm -fr $target_zip_path
        exit 1
    fi
    rm -f $target_zip_path
    echo $part_name
}

function attach() {
    part_name=$1
    msg=`echo "alter table $database.$table attach part '$part_name'" | curl -s "http://${DEFAULT_CLICKHOUSE_USER}:${DEFAULT_CLICKHOUSE_PASSWD}@${host}/?database=${database}" -d @-`
    if [[ "$msg" =~ "Exception" ]]; then
        echo "$msg"
        exit 1
    fi
}

disk=`pickDisk`

part_name=`downloadAndUnzip $disk $hdfs_part_path`

attach $part_name

echo "done"