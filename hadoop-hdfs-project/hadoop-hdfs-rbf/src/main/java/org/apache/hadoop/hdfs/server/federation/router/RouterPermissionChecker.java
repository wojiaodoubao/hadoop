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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.RouterINode;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Class that helps in checking permissions in Router-based federation.
 */
public class RouterPermissionChecker extends FSPermissionChecker {
  static final Logger LOG =
      LoggerFactory.getLogger(RouterPermissionChecker.class);

  /** Mount table default permission. */
  public static final short MOUNT_TABLE_PERMISSION_DEFAULT = 00755;

  /** Name of the super user. */
  private final String superUser;
  /** Name of the super group. */
  private final String superGroup;

  public RouterPermissionChecker(String user, String group,
      UserGroupInformation callerUgi) {
    super(user, group, callerUgi, null);
    this.superUser = user;
    this.superGroup = group;
  }

  /**
   * Whether a mount table entry can be accessed by the current context.
   *
   * @param mountTable
   *          MountTable being accessed
   * @param access
   *          type of action being performed on the mount table entry
   * @throws AccessControlException
   *           if mount table cannot be accessed
   */
  public void checkPermission(MountTable mountTable, FsAction access)
      throws AccessControlException {
    if (isSuperUser()) {
      return;
    }

    FsPermission mode = mountTable.getMode();
    if (getUser().equals(mountTable.getOwnerName())
        && mode.getUserAction().implies(access)) {
      return;
    }

    if (isMemberOfGroup(mountTable.getGroupName())
        && mode.getGroupAction().implies(access)) {
      return;
    }

    if (!getUser().equals(mountTable.getOwnerName())
        && !isMemberOfGroup(mountTable.getGroupName())
        && mode.getOtherAction().implies(access)) {
      return;
    }

    throw new AccessControlException(
        "Permission denied while accessing mount table "
            + mountTable.getSourcePath()
            + ": user " + getUser() + " does not have " + access.toString()
            + " permissions.");
  }

  /**
   * Check the superuser privileges of the current RPC caller. This method is
   * based on Datanode#checkSuperuserPrivilege().
   * @throws AccessControlException If the user is not authorized.
   */
  @Override
  public void checkSuperuserPrivilege() throws  AccessControlException {

    // Try to get the ugi in the RPC call.
    UserGroupInformation ugi = null;
    try {
      ugi = NameNode.getRemoteUser();
    } catch (IOException e) {
      // Ignore as we catch it afterwards
    }
    if (ugi == null) {
      LOG.error("Cannot get the remote user name");
      throw new AccessControlException("Cannot get the remote user name");
    }

    // Is this by the Router user itself?
    if (ugi.getShortUserName().equals(superUser)) {
      return;
    }

    // Is the user a member of the super group?
    if (ugi.getGroupsSet().contains(superGroup)) {
      return;
    }

    // Not a superuser
    throw new AccessControlException(
        ugi.getUserName() + " is not a super user");
  }

  /**
   * Check whether current user have permissions to access the path.
   * Traverse is always checked.
   *
   * Parent path means the parent directory for the path.
   * Ancestor path means the last (the closest) existing ancestor directory
   * of the path.
   * Note that if the parent path exists,
   * then the parent path and the ancestor path are the same.
   *
   * For example, suppose the path is "/foo/bar/baz".
   * No matter baz is a file or a directory,
   * the parent path is "/foo/bar".
   * If bar exists, then the ancestor path is also "/foo/bar".
   * If bar does not exist and foo exists,
   * then the ancestor path is "/foo".
   * Further, if both foo and bar do not exist,
   * then the ancestor path is "/".
   *
   * @param doCheckOwner Require user to be the owner of the path?
   * @param ancestorAccess The access required by the ancestor of the path.
   * @param parentAccess The access required by the parent of the path.
   * @throws AccessControlException
   *
   * Caller of this method must hold that lock.
   */
  public void checkPermission(RouterINode[] inodes, String path,
      boolean doCheckOwner, FsAction ancestorAccess, FsAction parentAccess)
      throws AccessControlException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ACCESS CHECK: " + this
          + ", doCheckOwner=" + doCheckOwner
          + ", ancestorAccess=" + ancestorAccess
          + ", parentAccess=" + parentAccess);
    }
    // check if (parentAccess != null) && file exists, then check sb
    // If resolveLink, the check is performed on the link target.
    int ancestorIndex = inodes.length - 2;
    byte[][] components = INode.getPathComponents(path);

    for(; ancestorIndex >= 0 && inodes[ancestorIndex] == null;
        ancestorIndex--);
    try {
      checkTraverse(inodes, components, ancestorIndex);
    } catch (UnresolvedPathException | ParentNotDirectoryException ex) {
      // must tunnel these exceptions out to avoid breaking interface for
      // external enforcer
      throw new TraverseAccessControlException(ex);
    }

    final RouterINode last = inodes[inodes.length - 1];
    if (parentAccess != null && parentAccess.implies(FsAction.WRITE)
        && inodes.length > 1 && last != null) {
      checkStickyBit(inodes, components, inodes.length - 2);
    }
    if (ancestorAccess != null && inodes.length > 1) {
      check(inodes, components, ancestorIndex, ancestorAccess);
    }
    if (parentAccess != null && inodes.length > 1) {
      check(inodes, components, inodes.length - 2, parentAccess);
    }
  }

  /**
   * @throws AccessControlException
   * @throws ParentNotDirectoryException
   * @throws UnresolvedPathException
   */
  private void checkTraverse(RouterINode[] inodes, byte[][] components,
      int last) throws AccessControlException, UnresolvedPathException,
      ParentNotDirectoryException {
    for (int i = 0; i <= last; i++) {
      checkIsDirectory(inodes[i], components, i);
      check(inodes, components, i, FsAction.EXECUTE);
    }
  }

  private void check(RouterINode[] inodes, byte[][] components, int i,
      FsAction access) throws AccessControlException {
    RouterINode inode = (i >= 0) ? inodes[i] : null;
    if (inode != null && !hasPermission(inode, access)) {
      throw new AccessControlException(
          toAccessControlString(inode, getPath(components, 0, i), access));
    }
  }

  // return whether access is permitted.  note it neither requires a path or
  // throws so the caller can build the path only if required for an exception.
  // very beneficial for subaccess checks!
  private boolean hasPermission(RouterINode inode, FsAction access) {
    if (inode == null) {
      return true;
    }
    final FsPermission mode = inode.getFsPermission();
    final AclStatus acl = inode.getAcl();
    if (acl != null && acl.getEntries().size() > 0) {
      // It's possible that the inode has a default ACL but no access ACL.
      AclEntry firstEntry = acl.getEntries().get(0);
      if (firstEntry.getScope() == AclEntryScope.ACCESS) {
        return hasAclPermission(inode, access, mode, acl);
      }
    }
    final FsAction checkAction;
    if (getUser().equals(inode.getUserName())) { //user class
      checkAction = mode.getUserAction();
    } else if (isMemberOfGroup(inode.getGroupName())) { //group class
      checkAction = mode.getGroupAction();
    } else { //other class
      checkAction = mode.getOtherAction();
    }
    return checkAction.implies(access);
  }

  /**
   * Checks requested access against an Access Control List.
   *
   * @param inode RouterINode accessed inode
   * @param access FsAction requested permission
   * @param mode FsPermission mode from inode
   * @param aclStatus AclFeature of inode
   * @throws AccessControlException if the ACL denies permission
   */
  private boolean hasAclPermission(RouterINode inode,
      FsAction access, FsPermission mode, AclStatus aclStatus) {
    boolean foundMatch = false;

    // Use owner entry from permission bits if user is owner.
    if (getUser().equals(inode.getUserName())) {
      if (mode.getUserAction().implies(access)) {
        return true;
      }
      foundMatch = true;
    }

    // Check named user and group entries if user was not denied by owner entry.
    AclEntry entry;
    if (!foundMatch) {
      for (int pos = 0; pos < aclStatus.getEntries().size(); pos++) {
        entry = aclStatus.getEntries().get(pos);
        if (entry.getScope() == AclEntryScope.DEFAULT) {
          break;
        }
        AclEntryType type = entry.getType();
        String name = entry.getName();
        if (type == AclEntryType.USER) {
          // Use named user entry with mask from permission bits applied if user
          // matches name.
          if (getUser().equals(name)) {
            FsAction masked = entry.getPermission().and(mode.getGroupAction());
            if (masked.implies(access)) {
              return true;
            }
            foundMatch = true;
            break;
          }
        } else if (type == AclEntryType.GROUP) {
          // Use group entry (unnamed or named) with mask from permission bits
          // applied if user is a member and entry grants access.  If user is a
          // member of multiple groups that have entries that grant access, then
          // it doesn't matter which is chosen, so exit early after first match.
          String group = name == null ? inode.getGroupName() : name;
          if (isMemberOfGroup(group)) {
            FsAction masked = entry.getPermission().and(mode.getGroupAction());
            if (masked.implies(access)) {
              return true;
            }
            foundMatch = true;
          }
        }
      }
    }

    // Use other entry if user was not denied by an earlier match.
    return !foundMatch && mode.getOtherAction().implies(access);
  }
}
