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
package org.apache.hadoop.hdfs.server.namenode.hfr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaSummary;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.Constants;
import org.apache.hadoop.fs.viewfs.MountpointRenewer;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsMountpointRenewer;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.server.namenode.FRHelper;
import org.apache.hadoop.hdfs.server.namenode.FederationRenamePeristAdapter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.FEDERATION_RENAME_WORK_PATH;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.FEDERATION_RENAME_WORK_PATH_DEFAULT;
import static org.apache.hadoop.hdfs.server.namenode.hfr.FederationRenameProcedure.FEDERATION_RENAME_CHECK_THREAD;
import static org.apache.hadoop.hdfs.server.namenode.hfr.FederationRenameProcedure.FEDERATION_RENAME_CHECK_THREAD_DEFAULT;
import static org.apache.hadoop.hdfs.server.namenode.hfr.FederationRenameProcedure.FEDERATION_RENAME_CHECK_TYPE;
import static org.apache.hadoop.hdfs.server.namenode.hfr.FederationRenameProcedure.FEDERATION_RENAME_CHECK_TYPE_DEFAULT;
import static org.apache.hadoop.hdfs.server.namenode.hfr.FederationRenameProcedure.LOG;
import static org.apache.hadoop.hdfs.server.namenode.hfr.ZkMountTableProcedure.FEDERATION_RENAME_PROCEDURE_MOUNT_CLUSTER;
import static org.apache.hadoop.hdfs.server.namenode.hfr.FederationRenameProcedure.FederationRenameJobContext;
import static org.apache.hadoop.hdfs.server.namenode.hfr.FederationRenameProcedure.PermissionStatus;

/**
 * FederationRenameUtils.
 */
public class FederationRenameUtils {

  static class OpenFileCloser implements Callable<Object> {
    final DistributedFileSystem fs;
    final RemoteIterator<OpenFileEntry> iterator;

    public OpenFileCloser(DistributedFileSystem fs,
        RemoteIterator<OpenFileEntry> iterator) {
      this.fs = fs;
      this.iterator = iterator;
    }

    @Override
    public Object call() throws Exception {
      Path file;
      while (true) {
        synchronized (iterator) {
          try {
            if (iterator.hasNext()) {
              file = new Path(iterator.next().getFilePath());
            } else {
              return null;
            }
          } catch (IOException e) {
            LOG.warn("Failed listing open files.", e);
            return null;
          }
        }
        try {
          fs.recoverLease(file);
        } catch (IOException e) {
          LOG.warn("Failed closing open file " + file, e);
        }
      }
    }
  }

  /**
   * Close all open files under basePath.
   *
   * @param fs the fs of target cluster.
   * @param basePath the basePath to close.
   * @param threadNum the thread number when closing open files.
   * @return whether all open files are closed.
   */
  public static boolean closeOpenFiles(DistributedFileSystem fs,
      String basePath, int threadNum) throws IOException {
    if (!basePath.equals("/")
        && basePath.charAt(basePath.length() - 1) == '/') {
      basePath += "/";
    }

    RemoteIterator<OpenFileEntry> iterator = fs.listOpenFiles(
        EnumSet.of(OpenFilesIterator.OpenFilesType.ALL_OPEN_FILES), basePath);
    if (!iterator.hasNext()) {
      return true;
    }
    ThreadPoolExecutor executor = new ThreadPoolExecutor(threadNum, threadNum,
        1, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(Integer.MAX_VALUE));
    CompletionService<Object> service =
        new ExecutorCompletionService<>(executor);
    try {
      for (int i = 0; i < threadNum; i++) {
        service.submit(new OpenFileCloser(fs, iterator));
      }
      int total = threadNum;
      while (total > 0) {
        try {
          service.take();
          total--;
        } catch (InterruptedException e) {
        }
      }
    } finally {
      executor.shutdownNow();
      if (iterator.hasNext()) {
        return false;
      } else {
        RemoteIterator<OpenFileEntry> hasOpenFiles = fs.listOpenFiles(
            EnumSet.of(OpenFilesIterator.OpenFilesType.ALL_OPEN_FILES),
            basePath);
        return !hasOpenFiles.hasNext();
      }
    }
  }

  public static Pattern CLUSTER_PATTERN =
      Pattern.compile("(?<cluster>^.*[prc|srv|tst]-.*)-\\d+$");

  public static void updateMountTable(FederationRenameJobContext jobContext)
      throws Exception {
    BasicInfo info = jobContext.info;
    Configuration conf = jobContext.conf;
    String clusterName = conf.get(FEDERATION_RENAME_PROCEDURE_MOUNT_CLUSTER);
    if (clusterName == null) {
      Matcher matcher = CLUSTER_PATTERN.matcher(info.src.getAuthority());
      if (matcher.find()) {
        clusterName = matcher.group("cluster");
      } else {
        throw new RuntimeException(
            "Couldn't find cluster name of mount table. Check "
                + FEDERATION_RENAME_PROCEDURE_MOUNT_CLUSTER + " in conf.");
      }
    }

    HdfsMountpointRenewer hmpr = new HdfsMountpointRenewer();
    hmpr.initialize(clusterName, conf, new MountpointRenewer.RenewMountpoint() {
      @Override
      public void doUpdateMountpoint() {
      }
    });

    String mountStr = new String(hmpr.getMptConfFromZookeeper(conf));
    Map<String, String> mountTable =
        HdfsMountpointRenewer.deserializeString2Mountpoint(mountStr);
    String key = Constants.CONFIG_VIEWFS_PREFIX + "." + clusterName + "."
        + Constants.CONFIG_VIEWFS_LINK + "." + info.src.getPath();
    mountTable.put(key, info.dst.toString());

    String newMountStr = HdfsMountpointRenewer
        .serializeMountpoint2String(mountTable, clusterName);
    hmpr.setMptConfToZookeeper(newMountStr.getBytes(), conf);
  }

  /**
   * Check dst parent exists, quota and permission.
   *
   * @param context the federation rename job context.
   */
  public static void sanityCheck(FederationRenameJobContext context)
      throws IOException {
    BasicInfo info = context.info;
    Configuration conf = context.conf;
    final DistributedFileSystem sFs =
        (DistributedFileSystem) FileSystem.get(info.src, conf);
    final DistributedFileSystem dFs =
        (DistributedFileSystem) FileSystem.get(info.dst, conf);

    // check whether dst parent exists.
    if (!dFs.exists(new Path(info.getDstPath()).getParent())) {
      throw new IOException(
          new Path(info.getDstPath()).getParent() + " doesn't exist.");
    }
    // check quota.
    Path srcPath = new Path(info.getSrcPath());
    Path dstPath = new Path(info.getDstPath());
    long nsDelta, dsDelta;
    QuotaSummary qs = sFs.getQuotaSummary(srcPath);
    if (qs.getNameCount() < 0 || qs.getSpaceConsumed() < 0) {
      ContentSummary cs = sFs.getContentSummary(srcPath);
      nsDelta = cs.getDirectoryCount() + cs.getFileCount();
      dsDelta = cs.getSpaceConsumed();
    } else {
      nsDelta = qs.getNameCount();
      dsDelta = qs.getSpaceConsumed();
    }
    Path checkPath = null;
    if (!dFs.exists(dstPath)) {
      dstPath = dstPath.getParent();
    } else {
      checkPath = dstPath;
    }
    while (checkPath != null) {
      qs = dFs.getQuotaSummary(checkPath);
      if (qs.getQuota() > 0 && qs.getNameCount() + nsDelta > qs.getQuota()) {
        throw new IOException("Quota exceed " + dstPath);
      }
      if (qs.getSpaceQuota() > 0 && qs.getSpaceConsumed() + dsDelta > qs
          .getSpaceQuota()) {
        throw new IOException("Quota exceed " + dstPath);
      }
      checkPath = checkPath.getParent();
    }

    // TODO:check permission. It's done by the admin manually now.
  }

  /**
   * Cancel x permission of src and save the permission to job context.
   *
   * @param context the federation rename job context.
   */
  public static void cancelAndSaveSrcExecutePermission(
      FederationRenameJobContext context) throws IOException {
    BasicInfo info = context.info;
    Configuration conf = context.conf;
    final DistributedFileSystem sFs =
        (DistributedFileSystem) FileSystem.get(info.src, conf);
    
    // save permission.
    if (context.pStatus == null) {
      context.pStatus = new PermissionStatus();
    }
    FileStatus status = sFs.getFileStatus(new Path(info.src.getPath()));
    context.pStatus.setFsPermission(status.getPermission());
    AclStatus acl = sFs.getAclStatus(new Path(info.src.getPath()));
    context.pStatus.setAcl(acl);

    // cancel x permission.
    sFs.setPermission(new Path(info.src.getPath()),
        FsPermission.createImmutable((short) 000));
  }

  /**
   * Build pool ids for FederationRename.
   */
  public static void buildPool(FederationRenameJobContext context) throws IOException {
    BasicInfo info = context.info;
    Configuration conf = context.conf;
    final DistributedFileSystem sFs =
        (DistributedFileSystem) FileSystem.get(info.src, conf);
    final DistributedFileSystem dFs =
        (DistributedFileSystem) FileSystem.get(info.dst, conf);

    String srcPoolId = sFs.getClient().getPoolId();
    String dstPoolId = dFs.getClient().getPoolId();
    info = new BasicInfo.Builder().setSrc(info.src).setDst(info.dst)
        .setCaller(info.caller).setSafeSrc(info.safeSrc)
        .setSafeDst(info.safeDst).setSrcPoolId(srcPoolId)
        .setDstPoolId(dstPoolId).setStartTime(info.startTime)
        .setContextURI(info.contextURI).setGlobalID(info.globalID).build();
    context.info.set(info);
  }

  /**
   * Build safeSrc and safeDst for FederationRename. If enableSafeSrc is true,
   * a path under FederationRename work path will be set to safeSrcPath. So is
   * enableSafeDst & safeDstPath.
   */
  public static void buildSafeSrcAndDstPath(FederationRenameJobContext context,
      boolean enableSafeSrc, boolean enableSafeDst)
      throws IOException, URISyntaxException {
    BasicInfo info = context.info;
    Configuration conf = context.conf;

    String workBase = conf.get(FEDERATION_RENAME_WORK_PATH,
        FEDERATION_RENAME_WORK_PATH_DEFAULT);
    Path workPath = new Path(workBase, info.globalID);
    Path safeSrcPath = new Path(info.src.getPath());
    if (enableSafeSrc) {
      safeSrcPath = new Path(workPath, new Path(info.src.getPath()).getName());
    }
    Path safeDstPath = new Path(info.dst.getPath());
    if (enableSafeDst) {
      safeDstPath = new Path(workPath, new Path(info.dst.getPath()).getName());
    }

    URI safeSrc = FederationRenameJobContext
        .getURI(new URI("hdfs://" + info.src.getAuthority() + "/"),
            safeSrcPath.toString());
    URI safeDst = FederationRenameJobContext
        .getURI(new URI("hdfs://" + info.dst.getAuthority() + "/"),
            safeDstPath.toString());
    info = new BasicInfo.Builder().setSrc(info.src).setDst(info.dst)
        .setCaller(info.caller).setSafeSrc(safeSrc).setSafeDst(safeDst)
        .setSrcPoolId(info.srcPoolId).setDstPoolId(info.dstPoolId)
        .setStartTime(info.startTime).setContextURI(info.contextURI)
        .setGlobalID(info.globalID).build();
    context.info.set(info);
  }

  /**
   * Save basic info to remote storage.
   */
  public static void saveBasicInfo(FederationRenameJobContext context)
      throws IOException {
    BasicInfo info = context.info;
    Configuration conf = context.conf;
    FileSystem contextFs = FileSystem.get(info.contextURI, conf);

    if (contextFs.exists(new Path(info.contextURI.getPath()))) {
      // For idempotent, move the context path to trash. We don't delete it
      // because an accident delete might kill the NameNode and it will be
      // very hard to recover. Even this method is only called in Prepare
      // phase so theoretically no intermediate files(except BASIC-INFO) would
      // be deleted. I still want moveToTrash to keep it safe.
      Trash.moveToAppropriateTrash(contextFs,
          new Path(info.contextURI.getPath()), context.conf);
    }
    contextFs.mkdirs(new Path(info.contextURI.getPath()));
    FederationRenamePeristAdapter frpa = new FRHelper();
    frpa.saveBasicInfo(info.getContextURI(), info);
  }

  public static void checkSameDirectories(Configuration conf,
      DistributedFileSystem sFs, DistributedFileSystem dFs, Path src,
      Path dst) throws IOException, InterruptedException {
    int threadNum = conf.getInt(FEDERATION_RENAME_CHECK_THREAD,
        FEDERATION_RENAME_CHECK_THREAD_DEFAULT);
    String checkType = conf.get(FEDERATION_RENAME_CHECK_TYPE,
        FEDERATION_RENAME_CHECK_TYPE_DEFAULT);
    if (checkType.equalsIgnoreCase("checksum")) {
      FederationRenameUtils
          .fullCheckSameDirectories(sFs, dFs, src, dst, threadNum);
    } else {
      FederationRenameUtils
          .checkSameDirectoriesByLocation(sFs, dFs, src, dst, threadNum);
    }
  }

  /**
   * A full check whether src path and dst path are the same. This method will
   * compare both the directory structure and the checksum.
   */
  public static void fullCheckSameDirectories(DistributedFileSystem srcHdfs,
      DistributedFileSystem dstHdfs, Path src, Path dst, int threadNum)
      throws IOException, InterruptedException {
    // Check base dirs, skip permission because src x permission is cancelled.
    FileStatus startStatus = srcHdfs.getFileStatus(src);
    FileStatus dstStatus = dstHdfs.getFileStatus(dst);
    ChecksumDirectoriesChecker
        .fullCheck(startStatus, dstStatus, srcHdfs, dstHdfs, true);
    // Check all children dirs.
    ChecksumDirectoriesChecker dChecker =
        new ChecksumDirectoriesChecker(threadNum, src.toUri().getPath(),
            dst.toUri().getPath(), srcHdfs, dstHdfs);
    if (startStatus.isDirectory()) {
      dChecker.add(srcHdfs.listStatus(startStatus.getPath()));
    }
    dChecker.execute();
  }

  /**
   * A simple way to check whether src path and dst path are the same.
   *
   * The assumption is "the hard link always success". So if a block is reported
   * to dst NameNode then there it no need to verify the checksum. We simply
   * collect all the dst locations here. If all the files could be found then
   * the verify succeeds.
   */
  public static void checkSameDirectoriesByLocation(DistributedFileSystem sfs,
      DistributedFileSystem dfs, Path src, Path dst, int threadNum)
      throws IOException, InterruptedException {
    FileStatus startStatus = sfs.getFileStatus(src);
    // Check all children dirs.
    LocationDirectoriesChecker lChecker =
        new LocationDirectoriesChecker(threadNum, src.toUri().getPath(),
            dst.toUri().getPath(), sfs, dfs);
    lChecker.add(startStatus);
    lChecker.execute();
  }

  static class LocationDirectoriesChecker
      extends ChecksumDirectoriesChecker {

    LocationDirectoriesChecker(int threadNum, String srcBase, String dstBase,
        DistributedFileSystem srcHdfs, DistributedFileSystem dstHdfs) {
      super(threadNum, srcBase, dstBase, srcHdfs, dstHdfs);
    }

    @Override
    public void consume(FileStatus sStatus, Object obj)
        throws IOException {
      if (sStatus.isDirectory()) {
        FileStatus[] children = srcHdfs.listStatus(sStatus.getPath());
        add(children);
      }
      Path dstPath = getDstPath(sStatus);
      FileStatus dStatus = dstHdfs.getFileStatus(dstPath);
      assert dStatus != null;
      locationCheck(sStatus, dStatus, dstHdfs);
    }

    static void locationCheck(FileStatus sStatus, FileStatus dStatus,
        DistributedFileSystem dstHdfs) throws IOException {
      String msg = sStatus.getPath() + " " + dStatus.getPath() + " ";
      if (sStatus.isFile()) {
        // Verify file len.
        check(sStatus.getLen() == dStatus.getLen(), msg + "length");
        // Verify dst locations.
        BlockLocation[] locations = dstHdfs
            .getFileBlockLocations(dStatus.getPath(), 0, dStatus.getLen());
        for (BlockLocation loc : locations) {
          if (loc.isCorrupt() || loc.getHosts().length < (
              dStatus.getReplication() / 2 + 1)) {
            throw new IOException("Dst File " + dStatus.getPath()
                + " doesn't have enough replicas! replication=" + dStatus
                .getReplication() + " expect=" + (dStatus.getReplication() / 2
                + 1) + " actual=" + loc.getHosts().length + " src file="
                + sStatus.getPath());
          }
        }
      }
    }
  }

  static class ChecksumDirectoriesChecker
      extends ParallelConsumer<FileStatus, Object> {

    String srcBase;
    String dstBase;
    DistributedFileSystem srcHdfs;
    DistributedFileSystem dstHdfs;

    ChecksumDirectoriesChecker(int threadNum, String srcBase, String dstBase,
        DistributedFileSystem srcHdfs, DistributedFileSystem dstHdfs) {
      super(threadNum);
      if (srcBase.length() > 0
          && srcBase.charAt(srcBase.length() - 1) == Path.SEPARATOR_CHAR) {
        this.srcBase = srcBase.substring(0, srcBase.length() - 1);
      } else {
        this.srcBase = srcBase;
      }
      if (dstBase.length() > 0
          && dstBase.charAt(dstBase.length() - 1) == Path.SEPARATOR_CHAR) {
        this.dstBase = dstBase.substring(0, dstBase.length() - 1);
      } else {
        this.dstBase = dstBase;
      }
      this.srcHdfs = srcHdfs;
      this.dstHdfs = dstHdfs;
    }

    @Override
    public void consume(FileStatus sStatus, Object obj)
        throws IOException {
      if (sStatus.isDirectory()) {
        FileStatus[] children = srcHdfs.listStatus(sStatus.getPath());
        add(children);
      }
      Path dstPath = getDstPath(sStatus);
      FileStatus dStatus = dstHdfs.getFileStatus(dstPath);
      fullCheck(sStatus, dStatus, srcHdfs, dstHdfs, false);
    }

    Path getDstPath(FileStatus status) {
      String srcStr = status.getPath().toUri().getPath();
      if (srcStr.length() == srcBase.length()) {
        return new Path(dstBase);
      }
      return new Path(dstBase, srcStr.substring(srcBase.length() + 1));
    }

    static void fullCheck(FileStatus sStatus, FileStatus dStatus,
        DistributedFileSystem srcHdfs, DistributedFileSystem dstHdfs,
        boolean skipPermission) throws IOException {
      String msg = sStatus.getPath() + " " + dStatus.getPath() + " ";
      check(sStatus.getPath().getName(), dStatus.getPath().getName(), msg + "name");
      if (!skipPermission) {
        check(sStatus.getPermission(), dStatus.getPermission(), msg + "permission");
      }
      check(sStatus.isDirectory() == dStatus.isDirectory(), msg + "is dir");
      check(sStatus.isFile() == dStatus.isFile(), msg + "is file");
      check(sStatus.isSymlink() == dStatus.isSymlink(), msg + "is symlink");
      if (!skipPermission) {
        check(sStatus.hasAcl() == dStatus.hasAcl(), msg + "acl");
        if (sStatus.hasAcl()) {
          AclStatus sAcl = srcHdfs.getAclStatus(sStatus.getPath());
          AclStatus dAcl = dstHdfs.getAclStatus(dStatus.getPath());
          // owner & group & sticky bit & entries
          check(sAcl, dAcl, msg + "acl");
        }
      }
      if (skipPermission || !sStatus.hasAcl()) {
        check(sStatus.getOwner(), dStatus.getOwner(), msg + "owner");
        check(sStatus.getGroup(), dStatus.getGroup(), msg + "group");
      }
      if (sStatus.isDirectory()) {
        QuotaUsage sQuota = srcHdfs.getQuotaUsage(sStatus.getPath());
        QuotaUsage dQuota = dstHdfs.getQuotaUsage(dStatus.getPath());
        check(sQuota, dQuota, msg + "quota");
      } else {
        FileChecksum sCheckSum = srcHdfs.getFileChecksum(sStatus.getPath());
        FileChecksum dCheckSum = dstHdfs.getFileChecksum(dStatus.getPath());
        check(sCheckSum, dCheckSum, msg + "checksum");
      }
      Map<String, byte[]> sXattrs = srcHdfs.getXAttrs(sStatus.getPath());
      Map<String, byte[]> dXattrs = dstHdfs.getXAttrs(dStatus.getPath());
      check(sXattrs.size() == dXattrs.size(), msg + "xattr");
      for (Map.Entry<String, byte[]> en : sXattrs.entrySet()) {
        check(Arrays.equals(en.getValue(), dXattrs.get(en.getKey())),
            msg + "xattr");
      }
    }

    static void check(Object obj0, Object obj1, String msg)
        throws IOException {
      if (obj0 == null) {
        check(obj1 == null, msg);
      } else {
        check(obj0.equals(obj1), msg);
      }
    }

    static void check(boolean bool, String msg) throws IOException {
      if (!bool) {
        throw new IOException(msg + " check failed.");
      }
    }
  }

  /**
   * Create dst path if it doesn't exist. Then preserve all attributes of src to
   * dst.
   */
  public static void mirrorSrcToDst(FederationRenameJobContext context,
      boolean skipPermAndAcl) throws IOException {
    BasicInfo info = context.info;
    Configuration conf = context.conf;
    final DistributedFileSystem sFs =
        (DistributedFileSystem) FileSystem.get(info.src, conf);
    final DistributedFileSystem dFs =
        (DistributedFileSystem) FileSystem.get(info.dst, conf);
    Path srcPath = new Path(info.src.getPath());
    Path dstPath = new Path(info.dst.getPath());
    if (!dFs.exists(dstPath)) {
      dFs.mkdirs(dstPath);
    }
    FileStatus status = sFs.getFileStatus(srcPath);
    if (!skipPermAndAcl) {
      // preserve acl & permission
      List<AclEntry> aclEntries = sFs.getAclStatus(srcPath).getEntries();
      aclEntries = AclUtil.getAclFromPermAndEntries(status.getPermission(),
          aclEntries != null ? aclEntries : Collections.emptyList());
      dFs.setPermission(dstPath, status.getPermission());
      dFs.setAcl(dstPath, aclEntries);
    } else {
      dFs.setPermission(dstPath, FsPermission.createImmutable((short) 0));
    }
    // preserve owner & group.
    dFs.setOwner(dstPath, status.getOwner(), status.getGroup());
    // preserve xAttributes
    Map<String, byte[]> oldXattr = dFs.getXAttrs(dstPath);
    for (String name : oldXattr.keySet()) {
      dFs.removeXAttr(dstPath, name);
    }
    Map<String, byte[]> newXattr = sFs.getXAttrs(srcPath);
    for (Map.Entry<String, byte[]> xattr : newXattr.entrySet()) {
      dFs.setXAttr(dstPath, xattr.getKey(), xattr.getValue());
    }
  }
}
