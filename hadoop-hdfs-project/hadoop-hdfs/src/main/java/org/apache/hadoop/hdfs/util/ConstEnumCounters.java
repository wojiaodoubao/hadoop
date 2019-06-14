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
package org.apache.hadoop.hdfs.util;

/**
 * Const Counters for an enum type.
 *
 * It's the const version of EnumCounters. Any modification ends with a
 * ConstEnumException.
 * See {@link EnumCounters}
 */
public class ConstEnumCounters<E extends Enum<E>> extends EnumCounters<E> {

  public static class ConstEnumException extends RuntimeException {
    public ConstEnumException(String msg) {
      super(msg);
    }
  }

  // Throwing cee when any modification happens, so we can avoid overheads of
  // constructing stack trace.
  public static final ConstEnumException cee =
      new ConstEnumException("modification on const.");

  public ConstEnumCounters(Class<E> enumClass, long defaultVal) {
    super(enumClass);
    forceReset(defaultVal);
  }

  /** Negate all counters. */
  public final void negation() {
    throw cee;
  }

  /** Set counter e to the given value. */
  public final void set(final E e, final long value) {
    throw cee;
  }

  /** Set this counters to that counters. */
  public final void set(final EnumCounters<E> that) {
    throw cee;
  }

  /** Reset all counters to zero. */
  public final void reset() {
    throw cee;
  }

  /** Add the given value to counter e. */
  public final void add(final E e, final long value) {
    throw cee;
  }

  /** Add that counters to this counters. */
  public final void add(final EnumCounters<E> that) {
    throw cee;
  }

  /** Subtract the given value from counter e. */
  public final void subtract(final E e, final long value) {
    throw cee;
  }

  /** Subtract this counters from that counters. */
  public final void subtract(final EnumCounters<E> that) {
    throw cee;
  }

  public final void reset(long val) {
    throw cee;
  }

  private final void forceReset(long val) {
    super.reset(val);
  }
}
