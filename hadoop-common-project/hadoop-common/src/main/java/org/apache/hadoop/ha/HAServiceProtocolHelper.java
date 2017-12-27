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
package org.apache.hadoop.ha;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ipc.RemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper for making {@link HAServiceProtocol} RPC calls. This helper
 * unwraps the {@link RemoteException} to specific exceptions.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HAServiceProtocolHelper {

  private static final Logger LOG =
          LoggerFactory.getLogger(HAServiceProtocolHelper.class);

  public static void monitorHealth(HAServiceProtocol svc,
      StateChangeRequestInfo reqInfo)
      throws IOException {
    try {
      svc.monitorHealth();
    } catch (RemoteException e) {
      throw e.unwrapRemoteException(HealthCheckFailedException.class);
    }
  }

  public static void transitionToActive(HAServiceProtocol svc,
      StateChangeRequestInfo reqInfo,Configuration conf)
      throws IOException {
    long endPoint = System.currentTimeMillis()+FailoverController.getTimeoutToNewActive(conf);
    try {
      svc.transitionToActive(reqInfo);// start transition
    } catch (RemoteException e) {
      LOG.warn("Failed start transition.");
      throw e.unwrapRemoteException(ServiceFailedException.class);
    }
    long checkInterval = FailoverController.getCheckIntervalToNewActive(conf);
    int retry = FailoverController.getRpcRetryTimesToNewActive(conf);
    int fail = 0;
    while (true) { // check transition periodically
      try {
        boolean inTransition = svc.transitionToActiveProgress();
        fail = 0;
        if (!inTransition || System.currentTimeMillis() >= endPoint) {
          break;
        }
        Thread.sleep(checkInterval);
      } catch (Exception e) {
        fail ++;
        if (e instanceof RemoteException) {
          e = ((RemoteException)e).unwrapRemoteException(ServiceFailedException.class);
        }
        if (fail<retry && e instanceof IOException) {
          if (fail<retry) {
            LOG.warn("check transition to active failed "
                    + fail + " times successively. " + (retry - fail)
                    + " times left to try.", e);
          } else {
            throw (IOException) e;
          }
        }
      }
    }
  }

  public static void transitionToStandby(HAServiceProtocol svc,
      StateChangeRequestInfo reqInfo)
      throws IOException {
    try {
      svc.transitionToStandby(reqInfo);
    } catch (RemoteException e) {
      throw e.unwrapRemoteException(ServiceFailedException.class);
    }
  }
}
