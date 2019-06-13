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
