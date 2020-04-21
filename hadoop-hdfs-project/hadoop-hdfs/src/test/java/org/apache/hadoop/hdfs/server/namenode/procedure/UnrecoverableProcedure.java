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
package org.apache.hadoop.hdfs.server.namenode.procedure;

import java.io.IOException;

/**
 * UnrecoverableProcedure will lose the handler if it's recovered.
 */
public class UnrecoverableProcedure extends Procedure {

  public interface Call {
    boolean execute(Procedure lastProcedure) throws RetryException, IOException;
  }

  private Call handler;

  public UnrecoverableProcedure() {}

  /**
   * The handler will be lost if the procedure is recovered.
   */
  public UnrecoverableProcedure(String name, long delay, Call handler) {
    super(name, delay);
    this.handler = handler;
  }

  @Override
  public boolean execute(Procedure lastProcedure) throws RetryException,
      IOException {
    if (handler != null) {
      return handler.execute(lastProcedure);
    } else {
      return true;
    }
  }
}
