#!/bin/bash
##############################################
#       author:sean                          #
#       last modify time:2023-11-12          #
##############################################

# 获取当前脚本所在目录
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#echo $script_name_without_extent
job_name=$1
#SQL
sql="INSERT INTO watermark ('name','last_time') VALUES ('$job_name','1997-07-09T01:00:00Z');"
db_file="$script_dir/../db/datax.db"

res=$(sqlite3 "$db_file" "$sql")

# 检查执行结果
if [ "$res" = "" ]; then
    echo "插入成功"
else
    echo "插入失败：$res"
fi