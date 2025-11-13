#!/bin/bash

disk_id_array=(1 2 3 4 5 6 7 8)
host=`hostname`

work_home=`cd $(dirname $0); pwd`

script_path=$0
script_name=${script_path##*/}
sscript_name=${script_name%.*}

script_home=`cd $(dirname $work_home); pwd`

script_data_logs_home=$work_home/logs/${sscript_name}
[ ! -d $script_data_logs_home ] && mkdir -p $script_data_logs_home

fname=`date +'%Y-%m-%d'`
flog=$script_data_logs_home/${fname}.log

for disk_id in "${disk_id_array[@]}"; do
    if [ ! -d /mnt/ssd/$disk_id ];then
        echo "Error! /mnt/ssd/$disk_id not exist!" | tee $flog && continue
    fi

    space_path=/mnt/ssd/$disk_id/storage/nebula

    for space in `ls $space_path`;do

        mkdir -p /cephfs/checkpoints/$host/$disk_id/$space

        if [ $space -eq 0 ];then

            cp -rf $space_path/0/data /cephfs/checkpoints/$host/$disk_id/$space | tee $flog
            continue
        fi

       if [ ! -d /cephfs/checkpoints/$host/$disk_id/$space/checkpoints ];then
            mkdir -p /cephfs/checkpoints/$host/$disk_id/$space/checkpoints | tee $flog
        fi

        for ckt in `ls $space_path/$space/checkpoints`;do

            if [ -d /cephfs/checkpoints/$host/$disk_id/$space/checkpoints/$ckt ];then
                continue;
            fi

            cp -rf $space_path/$space/checkpoints/$ckt /cephfs/checkpoints/$host/$disk_id/$space/checkpoints | tee $flog

        done

    done

done