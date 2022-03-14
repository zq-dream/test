#!/bin/bash

#script functions

#更新任务状态
#arg1:数据日期
#arg2:表名
#arg3:任务运行状态  $?
#arg4:任务阶段      start|end
function Table_Manager(){  

    DATA_DT=$1
    TABLE_NAME=$2
    
    if [ $3 -eq "0" ] ;then
        task_status="SUCCESS"
    else
        task_status="ERROR"
    fi
    
    case $4 in 
    "start")
        sql="
        INSERT INTO table_manager
        VALUES('${DATA_DT}','${TABLE_NAME}','RUNING',current_timestamp(),'9999-12-31 00:00:00')
        ;"
        ;;
    "end")
        sql="
        INSERT INTO table_manager
        VALUES('${DATA_DT}','${TABLE_NAME}','${task_status}',(select task_start_time from table_manager where data_dt='${DATA_DT}' and table_name='${TABLE_NAME}'),current_timestamp())
        ;"
        ;;
    esac

    echo $sql
    
    return 0
    
}
