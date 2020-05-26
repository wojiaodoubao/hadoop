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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class contains the basic information needed when Federation Balance.
 */
public class FedBalanceContext implements Writable {

  /* the source path in the source sub-cluster */
  private Path src;
  /* the target path in the target sub-cluster */
  private Path dst;
  /* the mount point to be balanced */
  private String mount;
  /* Force close all open files when there is no diff between src and dst */
  private boolean forceCloseOpenFiles;
  /* Disable write by setting the mount point readonly. */
  private boolean useMountReadOnly;
  /* The map number of the distcp job. */
  private int mapNum;
  /* The bandwidth limit of the distcp job(MB). */
  private int bandwidthLimit;
  /* Move source path to trash after all the data are sync to target. Otherwise
     delete the source directly. */
  private boolean moveToTrash;

  private Configuration conf;

  public FedBalanceContext() {}

  /**
   * Constructor of FedBalanceContext.
   *
   * @param src the source path in the source sub-cluster.
   * @param dst the target path in the target sub-cluster.
   * @param mount the mount point to be balanced.
   * @param conf the configuration.
   * @param forceCloseOpenFiles force close open files.
   * @param useMountReadOnly use mount point readonly to disable write.
   * @param mapNum the map number of the distcp job.
   * @param bandwidthLimit the bandwidth limit of the distcp job(MB).
   * @param moveToTrash Move source path to trash after all the data are sync to
   *                   target. Otherwise delete the source directly.
   */
  public FedBalanceContext(Path src, Path dst, String mount, Configuration conf,
      boolean forceCloseOpenFiles, boolean useMountReadOnly, int mapNum,
      int bandwidthLimit, boolean moveToTrash) {
    this.src = src;
    this.dst = dst;
    this.mount = mount;
    this.conf = conf;
    this.forceCloseOpenFiles = forceCloseOpenFiles;
    this.useMountReadOnly = useMountReadOnly;
    this.mapNum = mapNum;
    this.bandwidthLimit = bandwidthLimit;
    this.moveToTrash = moveToTrash;
  }

  public Configuration getConf() {
    return conf;
  }

  public Path getSrc() {
    return src;
  }

  public Path getDst() {
    return dst;
  }

  public String getMount() {
    return mount;
  }

  public boolean getForceCloseOpenFiles() {
    return forceCloseOpenFiles;
  }

  public boolean getUseMountReadOnly() {
    return useMountReadOnly;
  }

  public int getMapNum() {
    return mapNum;
  }

  public int getBandwidthLimit() {
    return bandwidthLimit;
  }

  public boolean getMoveToTrash() {
    return moveToTrash;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    conf.write(out);
    Text.writeString(out, src.toString());
    Text.writeString(out, dst.toString());
    Text.writeString(out, mount);
    out.writeBoolean(forceCloseOpenFiles);
    out.writeBoolean(useMountReadOnly);
    out.writeInt(mapNum);
    out.writeInt(bandwidthLimit);
    out.writeBoolean(moveToTrash);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    conf = new Configuration(false);
    conf.readFields(in);
    src = new Path(Text.readString(in));
    dst = new Path(Text.readString(in));
    mount = Text.readString(in);
    forceCloseOpenFiles = in.readBoolean();
    useMountReadOnly = in.readBoolean();
    mapNum = in.readInt();
    bandwidthLimit = in.readInt();
    moveToTrash = in.readBoolean();
  }
}