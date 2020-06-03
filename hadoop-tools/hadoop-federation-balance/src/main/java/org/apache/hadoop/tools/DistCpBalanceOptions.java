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
package org.apache.hadoop.tools;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * Command line options of DistCpFedBalance.
 */
public final class DistCpBalanceOptions {

  /**
   * The private construct protects this class from being instantiated.
   */
  private DistCpBalanceOptions() {}

  /**
   * If `true` the command runs in router mode. The source path is taken as
   * a mount point. It will disable write by setting the mount point
   * readonly. Otherwise the command works in normal federation mode. The
   * source path is taken as the full path. It will disable write by
   * cancelling the `x` permission of the source path. The default value
   * is `true`.
   */
  final static Option ROUTER =
      new Option("router", false, "Run in router-based federation mode.");

  /**
   * If true, in DIFF_DISTCP stage it will force close all open files when
   * there is no diff between the source path and the dst path. Otherwise
   * the DIFF_DISTCP stage will wait until there is no open files. The
   * default value is `false`.
   */
  final static Option FORCE_CLOSE_OPEN = new Option("forceCloseOpen", false,
      "Force close all open files if the src and dst are synced.");

  /**
   * Max number of maps to use during copy. DistCp will split work as equally
   * as possible among these maps.
   */
  final static Option MAP =
      new Option("map", true, "Max number of concurrent maps to use for copy");

  /**
   * Specify bandwidth per map in MB, accepts bandwidth as a fraction.
   */
  final static Option BANDWIDTH =
      new Option("bandwidth", true, "Specify bandwidth per map in MB.");

  final static Option DELAY_DURATION = new Option("delay", true,
      "Specify the delay duration in millie seconds.");

  /**
   * Move the source path to trash after all the data are sync to target, or
   * delete the source directly, or skip both trash and deletion.
   */
  final static Option TRASH = new Option("moveToTrash", true,
      "Move the source path to trash, or delete the source path directly,"
          + " or skip both trash and deletion."
          + " This accepts 3 values: trash, delete and skip.");

  final static Options CLI_OPTIONS = new Options();

  static {
    CLI_OPTIONS.addOption(ROUTER);
    CLI_OPTIONS.addOption(FORCE_CLOSE_OPEN);
    CLI_OPTIONS.addOption(MAP);
    CLI_OPTIONS.addOption(BANDWIDTH);
    CLI_OPTIONS.addOption(TRASH);
  }
}
