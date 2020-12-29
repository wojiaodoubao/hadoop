#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if ! declare -f hadoop_subcommand_fedbalance >/dev/null 2>/dev/null; then

  if [[ "${HADOOP_SHELL_EXECNAME}" = hadoop ]]; then
    hadoop_add_subcommand "fedbalance" client "balance data between sub-clusters"
  fi

  # this can't be indented otherwise shelldocs won't get it

## @description  fedbalance command for hadoop
## @audience     public
## @stability    stable
## @replaceable  yes
function hadoop_subcommand_fedbalance
{
  # shellcheck disable=SC2034
  HADOOP_CLASSNAME=org.apache.hadoop.hdfs.tools.fedbalance.FedBalance
  hadoop_add_to_classpath_tools hadoop-distcp
  hadoop_add_to_classpath_tools hadoop-federation-balance
}

fi