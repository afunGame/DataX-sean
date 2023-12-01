#!/bin/bash
##############################################
#       author:sean                          #
#       last modify time:2023-11-20          #
##############################################

# 获取当前脚本所在目录
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


#echo $script_name_without_extent
job_name=$1
#remove prefix
source_name=$(echo "$job_name" | awk -F '.' '{print $1}')
table_name=$(echo "$job_name" | awk -F '.' '{print $NF}')

#检查是否存在锁文件
if [ -f "$script_dir/lock/$job_name" ]; then
  echo "Last time job still running, abandon current job."
  exit 0
else
  # 创建一个锁文件
  touch "$script_dir/lock/$job_name"
fi


#
#Get config
#
db_file="$script_dir/../db/datax.db"

# Query last_time
query1="SELECT last_time FROM watermark WHERE name = '$job_name';"

last_time=$(sqlite3 "$db_file" "$query1")


echo "$job_name last_time:$last_time"

#Query config
query2="SELECT local_user,local_pwd,local_host,remote_user,remote_pwd,remote_host FROM source_config WHERE source = '$source_name';"

source_info=$(sqlite3 "$db_file" "$query2")

local_user=$(echo "$source_info" | awk -F '|' '{print $1}')
local_pwd=$(echo "$source_info" | awk -F '|' '{print $2}')
local_host=$(echo "$source_info" | awk -F '|' '{print $3}')

remote_user=$(echo "$source_info" | awk -F '|' '{print $4}')
remote_pwd=$(echo "$source_info" | awk -F '|' '{print $5}')
remote_host=$(echo "$source_info" | awk -F '|' '{print $6}')


# 获取当前时间并格式化为 ISO 8601
current_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

conf_path="$script_dir/../job/$table_name.json"

###start run sync job###
/usr/bin/python3 $script_dir/../bin/datax.py \
-p "-DstartDate=${last_time}        \
-DendDate=${current_time}           \
-Dlocal_dbuser=${local_user}        \
-Dlocal_passwd=${local_pwd}         \
-Dlocal_dbhost=${local_host}        \
-Dremote_dbuser=${remote_user}      \
-Dremote_passwd=${remote_pwd}       \
-Dremote_dbhost=${remote_host}      \
-DuserName=${user_name}             \
-DsourceName=${source_name}         \
-DchPwd=rf2ImJUC2.lOz               \
-DchHost=oci4ye33ou.ap-southeast-1.aws.clickhouse.cloud \
-DchUser=default"                  \
"$conf_path"

exit_code=$?

end_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

if [ $exit_code -eq 0  ];then
  echo "success"
  # 将当前时间追加到文件
  query3="UPDATE watermark SET last_time='$current_time' WHERE name='$job_name';"
  sqlite3 "$db_file" "$query3"
  notice="Job '$job_name' started at $current_time and successfully completed at $end_time. The synchronization process was successful!"
  #curl -X POST "https://api.telegram.org/bot6674167922:AAHqb9PyLXFmJzrTBDXaoMHFR7coF0xcwik/sendMessage" -d "chat_id=-1002092798074&text=$notice"
else
  echo "fail"
  notice="Unfortunately, the job '$job_name' initiated at $current_time encountered issues and concluded unsuccessfully at $end_time. The synchronization process failed."
  #curl -X POST "https://api.telegram.org/bot6674167922:AAHqb9PyLXFmJzrTBDXaoMHFR7coF0xcwik/sendMessage" -d "chat_id=-1002092798074&text=$notice"
fi

#remove lock file
rm -rf $script_dir/lock/$job_name