// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Offers functionality for implementations of
 * {@link AbstractNativeReference} which have an immutable reference to the
 * underlying native C++ object
 */
//@ThreadSafe
public abstract class AbstractImmutableNativeReference
    extends AbstractNativeReference {

  /**
   * A flag indicating whether the current {@code AbstractNativeReference} is
   * responsible to free the underlying C++ object
   */
  protected volatile boolean owningHandle_;

  protected AbstractImmutableNativeReference(final boolean owningHandle) {
    super(owningHandle);
    this.owningHandle_ = owningHandle;
  }

  @Override
  public boolean isOwningHandle() {
    return owningHandle_ && isAccessible();
  }

  /**
   * Releases this {@code AbstractNativeReference} from  the responsibility of
   * freeing the underlying native C++ object
   * <p>
   * This will prevent the object from attempting to delete the underlying
   * native object in {@code close()}. This must be used when another object
   * takes over ownership of the native object or both will attempt to delete
   * the underlying object when closed.
   * <p>
   * When {@code disOwnNativeHandle()} is called, {@code close()} will
   * subsequently take no action. As a result, incorrect use of this function
   * may cause a memory leak.
   * </p>
   */
  protected final void disOwnNativeHandle() {
    setReportLeak(false);
    owningHandle_ = false;
  }

  /**
   * The helper function of {@link AbstractImmutableNativeReference#close()}
   * which all subclasses of {@code AbstractImmutableNativeReference} must
   * implement to release their underlying native C++ objects.
   */
  protected abstract void disposeInternal(boolean owningHandle);

  @Override
  protected final void dispose() {
    this.disposeInternal(this.owningHandle_);
  }
}
