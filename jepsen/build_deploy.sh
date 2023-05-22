#!/bin/bash

sh ./build.sh ;
control run date;
control run dledger-control control-deploy;
control run dledger-node node-deploy;