#!/bin/bash

if [ -n "$1" ] ;then
    data_dt=$1
else
    echo "请传入日期参数"
    exit
fi

db_name=test_db
table_name=t5
temp_table_name=tmp

echo "=====================================================
db_name=${db_name}
table_name=${table_name}
temp_table_name=${temp_table_name}
data_dt= ${data_dt}
导入策略：拉链表，每日导入新增及变化的数据
=====================================================
"

sql="
TRUNCATE TABLE ${db_name}.${temp_table_name};

insert into ${db_name}.${temp_table_name}
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
    from ${db_name}.${table_name}
    where data_dt='3099-12-30'
) old full outer join
(
select 
    str_to_date('3099-12-30', '%Y-%m-%d') as data_dt,
    user_id,
    user_name,
    create_dt,
    operate_dt,
    str_to_date('${data_dt}', '%Y-%m-%d') as start_dt,
    str_to_date('3099-12-30', '%Y-%m-%d') as end_dt
from t4
where operate_dt='${data_dt}'
) new on old.user_id=new.user_id
;


ALTER TABLE ${db_name}.${table_name}
ADD PARTITION IF NOT EXISTS p${data_dt//-/} VALUES [('${data_dt}'), ('`date -d"${data_dt} 1 days" +"%Y-%m-%d"`'));

TRUNCATE TABLE ${db_name}.${table_name} PARTITION(p30991230);

insert into ${db_name}.${table_name}
select 
  ifnull(new_data_dt,old_data_dt) data_dt,
  ifnull(new_user_id,old_user_id) user_id,
  ifnull(new_user_name,old_user_name) user_name,
  ifnull(new_create_dt,old_create_dt) create_dt,
  ifnull(new_operate_dt,old_operate_dt) operate_dt,
  ifnull(new_start_dt,old_start_dt) start_dt,
  ifnull(new_end_dt,old_end_dt) end_dt
from ${db_name}.${temp_table_name} 
union all
select 
  str_to_date(date_sub('$data_dt',interval 1 day), '%Y-%m-%d') as data_dt,
  old_user_id user_id,
  old_user_name uaer_name,
  old_create_dt create_dt,
  old_operate_dt operate_dt,
  old_start_dt start_dt,
  str_to_date(date_sub('$data_dt',interval 1 day), '%Y-%m-%d') as end_dt
from ${db_name}.${temp_table_name}
where new_user_id is not null and old_user_id is not null
;


"

echo "${sql}"
