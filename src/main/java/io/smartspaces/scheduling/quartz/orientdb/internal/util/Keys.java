/*
 * Copyright (C) 2016 Keith M. Hughes
 * Forked from code (c) Michael S. Klishin, Alex Petrov, 2011-2015.
 * Forked from code from MuleSoft.
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

package io.smartspaces.scheduling.quartz.orientdb.internal.util;

import java.util.Date;

import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.utils.Key;

import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.internal.Constants;
import io.smartspaces.scheduling.quartz.orientdb.internal.Constants.LockType;

public class Keys {

  public static JobKey toJobKey(ODocument dbo) {
    return new JobKey((String) dbo.field(Constants.KEY_NAME), (String) dbo.field(Constants.KEY_GROUP));
  }

  public static TriggerKey toTriggerKey(ODocument dbo) {
    return new TriggerKey((String)dbo.field(Constants.KEY_NAME), (String)dbo.field(Constants.KEY_GROUP));
  }
}
