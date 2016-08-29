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

/**
 * A provider for locks for the various threads in the jobstore.
 * 
 * @author Keith M. Hughes
 */
public interface LockProvider {
  

  String LOCK_TRIGGER = "LOCK_TRIGGER";

  String LOCK_STATE_ACCESS = "STATE_ACCESS";


  /**
   * Obtain the requested lock for the current thread.
   * 
   * @param lockName
   *          the name of the lock
   * 
   * @return {@code true} if the lock was successfully obtained
   * 
   * @throws LockException
   *           something happened while obtaining the lock
   */
  boolean obtainLock(String lockName) throws LockException;

  /**
   * Release the lock.
   * 
   * <p>
   * A noop of the current thread does not own the lock.
   * 
   * @param lockName
   *          the name of the lock
   * 
   * @throws LockException
   *           something happened while releasing the lock
   */
  void releaseLock(String lockName) throws LockException;
}
