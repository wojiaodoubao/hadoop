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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.security.AccessControlException;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The Router INode is used in Router for permission check.
 */
public class RouterINode extends INode {

  private FileStatus status;
  private AclStatus acl;

  public RouterINode(INode parent, FileStatus status, AclStatus acl) {
    super(parent);
    this.status = status;
    this.acl = acl;
  }

  /**
   * Indicates the method is only a place holder. It shouldn't have any
   * implementation and should never be invoked.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.SOURCE)
  private @interface PlaceHolder {
  }

  @Override
  String getUserName(int snapshotId) {
    return status.getOwner();
  }

  @Override
  String getGroupName(int snapshotId) {
    return status.getGroup();
  }

  @Override
  FsPermission getFsPermission(int snapshotId) {
    return status.getPermission();
  }

  public AclStatus getAcl() {
    return acl;
  }

  @Override
  public boolean isDirectory() {
    return status.isDirectory();
  }

  @Override
  public boolean isSymlink() {
    return status.isSymlink();
  }

  @Override
  @PlaceHolder
  AclFeature getAclFeature(int snapshotId) {
    return null;
  }

  @Override
  @PlaceHolder
  public long getId() {
    return 0;
  }

  @Override
  @PlaceHolder
  public PermissionStatus getPermissionStatus(int snapshotId) {
    return null;
  }


  @Override
  @PlaceHolder
  void setUser(String user) {

  }

  @Override
  @PlaceHolder
  void setGroup(String group) {

  }

  @Override
  @PlaceHolder
  void setPermission(FsPermission permission) {

  }

  @Override
  @PlaceHolder
  void addAclFeature(AclFeature aclFeature) {

  }

  @Override
  @PlaceHolder
  void removeAclFeature() {

  }

  @Override
  @PlaceHolder
  XAttrFeature getXAttrFeature(int snapshotId) {
    return null;
  }

  @Override
  @PlaceHolder
  void addXAttrFeature(XAttrFeature xAttrFeature) {

  }

  @Override
  @PlaceHolder
  void removeXAttrFeature() {

  }

  @Override
  @PlaceHolder
  void recordModification(int latestSnapshotId) {

  }

  @Override
  @PlaceHolder
  public void cleanSubtree(ReclaimContext reclaimContext,
      int snapshotId, int priorSnapshotId) {

  }

  @Override
  @PlaceHolder
  public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {

  }

  @Override
  @PlaceHolder
  public ContentSummaryComputationContext computeContentSummary(
      int snapshotId, ContentSummaryComputationContext summary)
      throws AccessControlException {
    return null;
  }

  @Override
  @PlaceHolder
  public QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, boolean useCache, int lastSnapshotId) {
    return null;
  }

  @Override
  @PlaceHolder
  public void setLocalName(byte[] name) {

  }

  @Override
  long getModificationTime(int snapshotId) {
    return 0;
  }

  @Override
  @PlaceHolder
  public INode updateModificationTime(long mtime,
      int latestSnapshotId) {
    return null;
  }

  @Override
  @PlaceHolder
  public void setModificationTime(long modificationTime) {

  }

  @Override
  @PlaceHolder
  long getAccessTime(int snapshotId) {
    return 0;
  }

  @Override
  @PlaceHolder
  public void setAccessTime(long accessTime) {
    // do nothing.
  }

  @Override
  @PlaceHolder
  public byte getStoragePolicyID() {
    return 0;
  }

  @Override
  @PlaceHolder
  public byte getLocalStoragePolicyID() {
    return 0;
  }

  @Override
  @PlaceHolder
  public byte[] getLocalNameBytes() {
    return new byte[0];
  }

  @Override
  @PlaceHolder
  public short getFsPermissionShort() {
    return 0;
  }

  @Override
  @PlaceHolder
  public long getPermissionLong() {
    return 0;
  }
}
