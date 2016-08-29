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
package io.smartspaces.scheduling.quartz.orientdb.internal.trigger;

import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;

/**
 * The handler for misfires.
 * 
 * @author Keith M. Hughes
 */
public interface MisfireHandler {

  /**
   * Apply a misfire when recovering a trigger.
   *
   * @param trigger
   *          on which apply misfire logic
   * 
   * @return {@code true} when result of misfire is next fire time
   */
  boolean applyMisfireOnRecovery(OperableTrigger trigger) throws JobPersistenceException;

  boolean applyMisfire(OperableTrigger trigger) throws JobPersistenceException;

  /**
   * Calculate the earliest time in the past that a trigger will not be
   * considered misfired.
   * 
   * @return the earliest time for non-misfired classes
   */
  long getMisfireTime();

  /**
   * Scan for misfires.
   * 
   * <p>
   * This is a long running method and should be running inside its own thread.
   */
  void scanForMisfires();

  /**
   * Shutdown the misfire scan.
   */
  void shutdownScanForMisfires();

  boolean updateMisfiredTrigger(TriggerKey triggerKey, String newStateIfNotComplete,
      boolean forceState) throws JobPersistenceException;
}