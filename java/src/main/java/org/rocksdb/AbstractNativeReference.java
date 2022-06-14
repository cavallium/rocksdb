// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.internal.LeakDetection;
import io.netty5.buffer.api.internal.LifecycleTracer;
import io.netty5.buffer.api.internal.ResourceSupport;
import io.netty5.util.internal.SystemPropertyUtil;
import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AbstractNativeReference is the base-class of all RocksDB classes that have
 * a pointer to a native C++ {@code rocksdb} object.
 * <p>
 * AbstractNativeReference has the {@link AbstractNativeReference#close()}
 * method, which frees its associated C++ object.</p>
 * <p>
 * This function should be called manually, or even better, called implicitly using a
 * <a
 * href="https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">try-with-resources</a>
 * statement, when you are finished with the object. It is no longer
 * called automatically during the regular Java GC process via
 * {@link AbstractNativeReference#finalize()}.</p>
 * <p>
 * Explanatory note - When or if the Garbage Collector calls {@link Object#finalize()}
 * depends on the JVM implementation and system conditions, which the programmer
 * cannot control. In addition, the GC cannot see through the native reference
 * long member variable (which is the C++ pointer value to the native object),
 * and cannot know what other resources depend on it.
 * </p>
 */
public abstract class AbstractNativeReference implements AutoCloseable {

  static boolean leakDetectionEnabled = SystemPropertyUtil.getBoolean("io.netty5.buffer.leakDetectionEnabled", false);

  private static final Cleaner cleaner = Cleaner.create();

  private final AtomicInteger acquires; // Closed if negative.
  private final LifecycleTracer tracer;
  private final AtomicBoolean reportLeak;

  protected AbstractNativeReference(boolean reportLeak) {
    var tracer = LifecycleTracer.get();
    var acquires = new AtomicInteger(0);
    var reportLeakRef = new AtomicBoolean(reportLeak);
    String name = leakDetectionEnabled ? this.getClass().getName() : "rocksdb reference";
    cleaner.register(this, () -> {
      if (reportLeakRef.get() && acquires.get() >= 0) {
        LeakDetection.reportLeak(tracer, name);
      }
    });
    this.tracer = tracer;
    this.acquires = acquires;
    this.reportLeak = reportLeakRef;
  }

  /**
   * Returns true if we are responsible for freeing the underlying C++ object
   *
   * @return true if we are responsible to free the C++ object
   */
  protected abstract boolean isOwningHandle();

  public boolean isAccessible() {
    return acquires.get() >= 0;
  }


  /**
   * Record the current access location for debugging purposes.
   * This information may be included if the resource throws a life-cycle related exception, or if it leaks.
   * If this resource has already been closed, then this method has no effect.
   *
   * @param hint An optional hint about this access and its context. May be {@code null}.
   */
  public void touch(Object hint) {
    if (isAccessible()) {
      tracer.touch(hint);
    }
  }

  /**
   * Decrement the reference count, and dispose of the resource if the last reference is closed.
   * <p>
   * Note, this method is not thread-safe because Resources are meant to be thread-confined.
   * <p>
   * Subclasses who wish to attach behaviour to the close action should override the {@link #makeInaccessible()}
   * method instead, or make it part of their drop implementation.
   *
   * @throws IllegalStateException If this Resource has already been closed.
   */
  @Override
  public final void close() {
    if (acquires.get() == -1) {
      throw attachTrace(new IllegalStateException("Double-free: Resource already closed and dropped."));
    }
    int acq = acquires.getAndDecrement();
    tracer.close(acq);
    if (acq == 0) {
      // The 'acquires' was 0, now decremented to -1, which means we need to drop.
      tracer.drop(0);
      try {
        this.dispose();
      } finally {
        makeInaccessible();
      }
    }
  }

  /**
   * Called once
   */
  protected abstract void dispose();

  /**
   * Attach a trace of the life-cycle of this object as suppressed exceptions to the given throwable.
   *
   * @param throwable The throwable to attach a life-cycle trace to.
   * @param <E> The concrete exception type.
   * @return The given exception, which can then be thrown.
   */
  protected <E extends Throwable> E attachTrace(E throwable) {
    return tracer.attachTrace(throwable);
  }

  /**
   * Called when this resource needs to be considered inaccessible.
   * This is called at the correct points, by the {@link ResourceSupport} class,
   * when the resource is being closed or sent,
   * and can be used to set further traps for accesses that makes accessibility checks cheap.
   * There would normally not be any reason to call this directly from a sub-class.
   */
  protected void makeInaccessible() {
  }

  protected final boolean getReportLeak() {
    return reportLeak.get();
  }

  protected final void setReportLeak(boolean reportLeak) {
    this.reportLeak.set(reportLeak);
  }
}
