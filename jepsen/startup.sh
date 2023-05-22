#!/bin/bash

nohup java -jar ./register-dledger.jar server $@ >> register-dledger.log 2>&1 &