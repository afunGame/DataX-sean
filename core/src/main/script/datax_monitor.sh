#!/bin/bash
##############################################
#       author:sean                          #
#       last modify time:2023-11-20          #
##############################################
  
step=1 #间隔的秒数，不能大于60  
  
for (( i = 0; i < 60; i=(i+step) )); do  
    /bin/bash /opt/datax_oid/script/run_datax.sh $1
    sleep $step  
done
  
exit 0