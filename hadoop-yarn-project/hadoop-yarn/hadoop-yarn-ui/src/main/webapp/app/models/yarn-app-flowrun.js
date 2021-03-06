/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import DS from 'ember-data';
import Converter from 'yarn-ui/utils/converter';

export default DS.Model.extend({
  appId: DS.attr('string'),
  type: DS.attr('string'),
  uid: DS.attr('string'),
  metrics: DS.attr('array'),
  startedTs: DS.attr('number'),
  finishedTs: DS.attr('number'),
  state: DS.attr('string'),
  cpuVCores: DS.attr('number'),
  memoryUsed: DS.attr('number'),

  elapsedTs: function() {
    return this.get('finishedTs') - this.get('startedTs');
  }.property('startedTs', 'finishedTs'),

  getCpuVCoresVizDataForBarChart: function() {
    return {
      label: this.get('appId'),
      value: this.get('cpuVCores'),
      tooltip: this.get("appId") + "<br>" + 'CPU VCores: ' + this.get('cpuVCores')
    };
  },

  getMemoryVizDataForBarChart: function() {
    return {
      label: this.get('appId'),
      value: this.get('memoryUsed'),
      tooltip: this.get("appId") + "<br>" + 'Memory Used: ' + Converter.memoryBytesToMB(this.get('memoryUsed'))
    };
  }
});
