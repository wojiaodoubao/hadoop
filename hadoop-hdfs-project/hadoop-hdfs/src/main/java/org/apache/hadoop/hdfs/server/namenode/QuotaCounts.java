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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.util.ConstEnumCounters;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.ConstEnumCounters.ConstEnumException;

/**
 * Counters for namespace, storage space and storage type space quota and usage.
 */
public class QuotaCounts {

  public final static EnumCounters<Quota> QUOTA_RESET;
  public final static EnumCounters<Quota> QUOTA_DEFAULT;
  public final static EnumCounters<StorageType> STORAGE_TYPE_RESET;
  public final static EnumCounters<StorageType> STORAGE_TYPE_DEFAULT;

  static {
    QUOTA_DEFAULT = new ConstEnumCounters(Quota.class, 0);
    QUOTA_RESET = new ConstEnumCounters(Quota.class, HdfsConstants.QUOTA_RESET);
    STORAGE_TYPE_DEFAULT = new ConstEnumCounters(StorageType.class, 0);
    STORAGE_TYPE_RESET =
        new ConstEnumCounters(StorageType.class, HdfsConstants.QUOTA_RESET);
  }

  interface CounterFunc<T extends Enum<T>> {
    void func(EnumCounters<T> ec) throws ConstEnumException;
  }

  static <T extends Enum<T>> EnumCounters<T> modify(EnumCounters<T> ec,
      CounterFunc<T> cf) {
    try {
      cf.func(ec);
    } catch (ConstEnumException cee) {
      ec = (EnumCounters<T>) ec.clone();
      cf.func(ec);
    }
    return ec;
  }

  // Name space and storage space counts (HDFS-7775 refactors the original disk
  // space count to storage space counts)
  @VisibleForTesting
  protected EnumCounters<Quota> nsSsCounts;
  // Storage type space counts
  @VisibleForTesting
  protected EnumCounters<StorageType> tsCounts;

  public static class Builder {
    private EnumCounters<Quota> nsSsCounts;
    private EnumCounters<StorageType> tsCounts;

    public Builder() {
      this.nsSsCounts = QUOTA_DEFAULT;
      this.tsCounts = STORAGE_TYPE_DEFAULT;
    }

    public Builder nameSpace(long val) {
      if (val == HdfsConstants.QUOTA_RESET
          && nsSsCounts.get(Quota.STORAGESPACE) == HdfsConstants.QUOTA_RESET) {
        nsSsCounts = QUOTA_RESET;
      } else if (val == 0 && nsSsCounts.get(Quota.STORAGESPACE) == 0) {
        nsSsCounts = QUOTA_DEFAULT;
      } else {
        nsSsCounts = modify(nsSsCounts, ec->ec.set(Quota.NAMESPACE, val));
      }
      return this;
    }

    public Builder storageSpace(long val) {
      if (val == HdfsConstants.QUOTA_RESET
          && nsSsCounts.get(Quota.NAMESPACE) == HdfsConstants.QUOTA_RESET) {
        this.nsSsCounts = QUOTA_RESET;
      } else if (val == 0 && nsSsCounts.get(Quota.NAMESPACE) == 0) {
        this.nsSsCounts = QUOTA_DEFAULT;
      } else {
        nsSsCounts = modify(nsSsCounts, ec -> ec.set(Quota.STORAGESPACE, val));
      }
      return this;
    }

    public Builder typeSpaces(EnumCounters<StorageType> val) {
      if (val != null) {
        if (val == STORAGE_TYPE_DEFAULT || val == STORAGE_TYPE_RESET) {
          tsCounts = val;
        } else {
          tsCounts = modify(tsCounts, ec -> ec.set(val));
        }
      }
      return this;
    }

    public Builder typeSpaces(long val) {
      if (val == HdfsConstants.QUOTA_RESET) {
        tsCounts = STORAGE_TYPE_RESET;
      } else if (val == 0) {
        tsCounts = STORAGE_TYPE_DEFAULT;
      } else {
        tsCounts = modify(tsCounts, ec -> ec.reset(val));
      }
      return this;
    }

    public Builder quotaCount(QuotaCounts that) {
      nsSsCounts = modify(nsSsCounts, ec -> ec.set(that.nsSsCounts));
      tsCounts = modify(tsCounts, ec -> ec.set(that.tsCounts));
      return this;
    }

    public QuotaCounts build() {
      return new QuotaCounts(this);
    }
  }

  private QuotaCounts(Builder builder) {
    this.nsSsCounts = builder.nsSsCounts;
    this.tsCounts = builder.tsCounts;
  }

  public QuotaCounts add(QuotaCounts that) {
    nsSsCounts = modify(nsSsCounts, ec -> ec.add(that.nsSsCounts));
    tsCounts = modify(tsCounts, ec -> ec.add(that.tsCounts));
    return this;
  }

  public QuotaCounts subtract(QuotaCounts that) {
    nsSsCounts = modify(nsSsCounts, ec->ec.subtract(that.nsSsCounts));
    tsCounts = modify(tsCounts, ec->ec.subtract(that.tsCounts));
    return this;
  }

  /**
   * Returns a QuotaCounts whose value is {@code (-this)}.
   *
   * @return {@code -this}
   */
  public QuotaCounts negation() {
    QuotaCounts ret = new QuotaCounts.Builder().quotaCount(this).build();
    ret.nsSsCounts.negation();
    ret.tsCounts.negation();
    return ret;
  }

  public long getNameSpace(){
    return nsSsCounts.get(Quota.NAMESPACE);
  }

  public void setNameSpace(long nameSpaceCount) {
    if (nameSpaceCount == HdfsConstants.QUOTA_RESET
        && nsSsCounts.get(Quota.STORAGESPACE) == HdfsConstants.QUOTA_RESET) {
      nsSsCounts = QUOTA_RESET;
    } else if (nameSpaceCount == 0 && nsSsCounts.get(Quota.STORAGESPACE) == 0) {
      nsSsCounts = QUOTA_DEFAULT;
    } else {
      nsSsCounts =
          modify(nsSsCounts, ec -> ec.set(Quota.NAMESPACE, nameSpaceCount));
    }
  }

  public void addNameSpace(long nsDelta) {
    nsSsCounts = modify(nsSsCounts, ec->ec.add(Quota.NAMESPACE, nsDelta));
  }

  public long getStorageSpace(){
    return nsSsCounts.get(Quota.STORAGESPACE);
  }

  public void setStorageSpace(long spaceCount) {
    if (spaceCount == HdfsConstants.QUOTA_RESET
        && nsSsCounts.get(Quota.NAMESPACE) == HdfsConstants.QUOTA_RESET) {
      nsSsCounts = QUOTA_RESET;
    } else if (spaceCount == 0 && nsSsCounts.get(Quota.NAMESPACE) == 0) {
      nsSsCounts = QUOTA_DEFAULT;
    } else {
      nsSsCounts =
          modify(nsSsCounts, ec -> ec.set(Quota.STORAGESPACE, spaceCount));
    }
  }

  public void addStorageSpace(long dsDelta) {
    nsSsCounts = modify(nsSsCounts, ec->ec.add(Quota.STORAGESPACE, dsDelta));
  }

  public EnumCounters<StorageType> getTypeSpaces() {
    EnumCounters<StorageType> ret =
        new EnumCounters<StorageType>(StorageType.class);
    ret.set(tsCounts);
    return ret;
  }

  void setTypeSpaces(EnumCounters<StorageType> that) {
    if (that != null) {
      tsCounts = modify(tsCounts, ec -> ec.set(that));
    }
  }

  long getTypeSpace(StorageType type) {
    return this.tsCounts.get(type);
  }

  void setTypeSpace(StorageType type, long spaceCount) {
    tsCounts = modify(tsCounts, ec->ec.set(type, spaceCount));
  }

  public void addTypeSpace(StorageType type, long delta) {
    tsCounts = modify(tsCounts, ec->ec.add(type, delta));
  }

  public boolean anyNsSsCountGreaterOrEqual(long val) {
    if (nsSsCounts == QUOTA_DEFAULT && val >= 0) {
      return false;
    } else if (nsSsCounts == QUOTA_RESET && val >= HdfsConstants.QUOTA_RESET) {
      return false;
    }
    return nsSsCounts.anyGreaterOrEqual(val);
  }

  public boolean anyTypeSpaceCountGreaterOrEqual(long val) {
    if (tsCounts == STORAGE_TYPE_DEFAULT && val >= 0) {
      return false;
    } else if (tsCounts == STORAGE_TYPE_RESET
        && val >= HdfsConstants.QUOTA_RESET) {
      return false;
    }
    return tsCounts.anyGreaterOrEqual(val);
  }

  @Override
  public String toString() {
    return "name space=" + getNameSpace() +
        "\nstorage space=" + getStorageSpace() +
        "\nstorage types=" + getTypeSpaces();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj == null || !(obj instanceof QuotaCounts)) {
      return false;
    }
    final QuotaCounts that = (QuotaCounts)obj;
    return this.nsSsCounts.equals(that.nsSsCounts)
        && this.tsCounts.equals(that.tsCounts);
  }

  @Override
  public int hashCode() {
    assert false : "hashCode not designed";
    return 42; // any arbitrary constant will do
  }

}
