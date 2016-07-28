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

package io.smartspaces.scheduling.quartz.orientdb.util;

import static io.smartspaces.scheduling.quartz.orientdb.util.Keys.KEY_GROUP;

import java.util.Collection;

import org.quartz.impl.matchers.GroupMatcher;

/**
 * A helper for piecing queries together.
 */
public class QueryHelper {

  public String matchingKeysConditionFor(GroupMatcher<?> matcher) {
    final String compareToValue = matcher.getCompareToValue();

    switch (matcher.getCompareWithOperator()) {
      case EQUALS:
        return KEY_GROUP + " = '" + compareToValue + "'";
      case STARTS_WITH:
        return KEY_GROUP + " MATCHES '^" + compareToValue + ".*'";
      case ENDS_WITH:
        return KEY_GROUP + "MATCHES '.*" + compareToValue + "$'";
      case CONTAINS:
        return KEY_GROUP + "MATCHES '" + compareToValue + "'";
    }

    return "";
  }

  /**
   * Create a query extension that will check if the group name is in a
   * collection of groups.
   * 
   * @param groups
   *          the group names to check in
   * 
   * @return a SQL clause that checks for the group name of a record to be in a
   *         collection of groups
   */
  public String inGroups(Collection<String> groups) {
    StringBuilder builder = new StringBuilder();

    for (String group : groups) {
      if (builder.length() != 0) {
        builder.append(", ");
      }
      builder.append("'").append(group).append("'");
    }
    return KEY_GROUP + " IN (" + builder + ")";
  }
}
