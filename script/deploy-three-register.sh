#!/bin/bash
#
# Copyright 2017-2022 The DLedger Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

nohup java -jar ../example/target/dledger-example.jar register --peers "n0-localhost:20911;n1-localhost:20912;n2-localhost:20913" --id n0 --metrics-exporter-type PROM  --metrics-prom-export-port 5557  > register-n0.log  2>&1   &

nohup java -jar ../example/target/dledger-example.jar register --peers "n0-localhost:20911;n1-localhost:20912;n2-localhost:20913" --id n1 --metrics-exporter-type PROM  --metrics-prom-export-port 5558  > register-n1.log  2>&1   &

nohup java -jar ../example/target/dledger-example.jar register --peers "n0-localhost:20911;n1-localhost:20912;n2-localhost:20913" --id n2 --metrics-exporter-type PROM  --metrics-prom-export-port 5559  > register-n2.log  2>&1   &

