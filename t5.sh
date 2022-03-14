#!/bin/bash

if [ -n "$1" ] ;then
    DATA_DT=$1
else
    echo "请传入日期参数"
    exit
fi

source /home/dg/zhangqiang/functions.sh

DB_NAME=test_db
TABLE_NAME=t5
TEMP_TABLE_NAME=tmp

echo "=====================================================
DB_NAME=${DB_NAME}
TABLE_NAME=${TABLE_NAME}
TEMP_TABLE_NAME=${TEMP_TABLE_NAME}
DATA_DT= ${DATA_DT}
导入策略：拉链表，每日导入新增及变化的数据
=====================================================
"

Table_Manager ${DATA_DT} ${TABLE_NAME} $? start

SQL="
    TRUNCATE TABLE ${DB_NAME}.${TEMP_TABLE_NAME};
    
    insert into ${DB_NAME}.${TEMP_TABLE_NAME}
    select 
    old.data_dt old_data_dt,
    old.user_id old_user_id,
    old.user_name old_user_name,
    old.create_dt old_create_dt,
    old.operate_dt old_operate_dt,
    old.start_dt old_start_dt,
    old.end_dt old_end_dt,
    new.data_dt new_data_dt,
    new.user_id new_user_id,
    new.user_name new_user_name,
    new.create_dt new_create_dt,
    new.operate_dt new_operate_dt,
    new.start_dt new_start_dt,
    new.end_dt new_end_dt
    from 
    (
        select data_dt,user_id,user_name,create_dt, operate_dt,start_dt,end_dt
        from ${DB_NAME}.${TABLE_NAME}
        where data_dt='3099-12-30'
    ) old full outer join
    (
    select 
        str_to_date('3099-12-30', '%Y-%m-%d') as data_dt,
        user_id,
        user_name,
        create_dt,
        operate_dt,
        str_to_date('${DATA_DT}', '%Y-%m-%d') as start_dt,
        str_to_date('3099-12-30', '%Y-%m-%d') as end_dt
    from t4
    where operate_dt='${DATA_DT}'
    ) new on old.user_id=new.user_id
    ;
    
    
    ALTER TABLE ${DB_NAME}.${TABLE_NAME}
    ADD PARTITION IF NOT EXISTS p${DATA_DT//-/} VALUES [('${DATA_DT}'), ('`date -d"${DATA_DT} 1 days" +"%Y-%m-%d"`'));
    
    TRUNCATE TABLE ${DB_NAME}.${TABLE_NAME} PARTITION(p30991230);
    
    insert into ${DB_NAME}.${TABLE_NAME}
    select 
    ifnull(new_data_dt,old_data_dt) data_dt,
    ifnull(new_user_id,old_user_id) user_id,
    ifnull(new_user_name,old_user_name) user_name,
    ifnull(new_create_dt,old_create_dt) create_dt,
    ifnull(new_operate_dt,old_operate_dt) operate_dt,
    ifnull(new_start_dt,old_start_dt) start_dt,
    ifnull(new_end_dt,old_end_dt) end_dt
    from ${DB_NAME}.${TEMP_TABLE_NAME} 
    union all
    select 
    str_to_date(date_sub('${DATA_DT}',interval 1 day), '%Y-%m-%d') as data_dt,
    old_user_id user_id,
    old_user_name uaer_name,
    old_create_dt create_dt,
    old_operate_dt operate_dt,
    old_start_dt start_dt,
    str_to_date(date_sub('${DATA_DT}',interval 1 day), '%Y-%m-%d') as end_dt
    from ${DB_NAME}.${TEMP_TABLE_NAME}
    where new_user_id is not null and old_user_id is not null
    ;"

echo "${SQL}"

Table_Manager ${DATA_DT} ${TABLE_NAME} $? end
