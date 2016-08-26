/*
 * Copyright (C) 2016 Keith M. Hughes
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.smartspaces.scheduling.quartz.orientdb.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * A lock provider that does everything in memory.
 * 
 * @author Keith M. Hughes
 */
public class SimpleLockProvider implements LockProvider {

  /**
   * The logger for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(SimpleLockProvider.class);

  /**
   * The locks owned by the current htread.
   */
  private ThreadLocal<Set<String>> threadLocksSource = new ThreadLocal<Set<String>>();

  /**
   * The locks to own.
   */
  private Set<String> locks = new HashSet<String>();

  @Override
  public synchronized boolean obtainLock(String lockName) throws LockException {
    String threadName = Thread.currentThread().getName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Thread {} is attempting to get lock {}", threadName, lockName);
    }

    if (!isLockOwner(lockName)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Thread {} is waiting for lock {}", threadName, lockName);
      }
      while (locks.contains(lockName)) {
        try {
          this.wait();
        } catch (InterruptedException ie) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Thread {} did not get lock {}", threadName, lockName);
          }
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Thread {} got lock {}", threadName, lockName);
      }
      getThreadLocks().add(lockName);
      locks.add(lockName);
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Thread {} already owns lock {}", threadName, lockName);
    }

    return true;
  }

  @Override
  public synchronized void releaseLock(String lockName) throws LockException {
    String threadName = Thread.currentThread().getName();
    if (isLockOwner(lockName)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Thread {} is returning lock {}", threadName, lockName);
      }
      getThreadLocks().remove(lockName);
      locks.remove(lockName);
      this.notifyAll();
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Thread {} is attempting to return lock {} but it doesn't own it", threadName,
          lockName, new Exception("stack trace for bad return"));
    }
  }

  /**
   * Get the locks for the current thread.
   * 
   * @return the locks owned by the current thread.
   */
  private Set<String> getThreadLocks() {
    Set<String> threadLocks = threadLocksSource.get();
    if (threadLocks == null) {
      threadLocks = new HashSet<String>();
      threadLocksSource.set(threadLocks);
    }

    return threadLocks;
  }

  /**
   * Is the current thread the owner of the given lock?
   * 
   * @param lockName
   *          the name of the lock
   * 
   * @return {@code true} if the current thread owns the lock
   */
  private boolean isLockOwner(String lockName) {
    return getThreadLocks().contains(lockName);
  }
}
