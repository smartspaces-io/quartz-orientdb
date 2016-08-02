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

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * A collection of methods to help working with orientDB documents.
 * 
 * @author Keith M. Hughes
 */
public class ODocumentHelper {

  /**
   * Get a boolean value from a document, using a default value if it doesn't
   * exist.
   * 
   * @param document
   *          the document to get the field from
   * @param fieldName
   *          the name of the field
   * @param defaultValue
   *          the default vale if the field is not in the document
   *          
   * @return the boolean value
   */
  public static boolean getBooleanField(ODocument document, String fieldName, boolean defaultValue) {
    Boolean value = document.field(fieldName);
    if (value != null) {
      return value;
    } else {
      return defaultValue;
    }
  }
}
