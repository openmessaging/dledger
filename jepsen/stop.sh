#!/bin/bash

pid=`ps -ef |grep register-dledger |grep java |awk -F' ' '{print $2}'`
if [ "$pid" != "" ]
then
    echo "kill $pid"
    kill $pid
fi