#!/bin/bash

nohup java -jar ./target/register-dledger.jar $@ >> register-dledger.log 2>&1 &