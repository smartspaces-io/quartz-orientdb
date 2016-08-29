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

package io.smartspaces.scheduling.quartz.orientdb.internal.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.quartz.JobPersistenceException;

/**
 * The connector to the OrientDB database.
 * 
 * @author Keith M. HUghes
 */
public interface OrientDbConnector {

  void shutdown();

  /**
   * Get the connection to the database.
   * 
   * <p>
   * The first time the connection is obtained in a thread, a transaction will
   * be started.
   * 
   * @return the connection
   */
  ODatabaseDocumentTx getConnection();

  /**
   * Do a method in a transaction without a lock.
   * 
   * @param method
   *          the method to run in the transaction
   * 
   * @return the result of the method
   * 
   * @throws JobPersistenceException
   *           something bad happened
   */
  <T> T doInTransactionWithoutLock(TransactionMethod<T> method) throws JobPersistenceException;

  /**
   * Do a method in a transaction.
   * 
   * @param lockRequired
   *          the name of the lock required
   * @param method
   *          the method to run in the transaction
   * 
   * @return the result of the method
   * 
   * @throws JobPersistenceException
   *           something bad happened
   */
  <T> T doInTransaction(String lockRequired, TransactionMethod<T> method)
      throws JobPersistenceException;

  public interface TransactionMethod<T> {
    T doInTransaction() throws JobPersistenceException;
  }

}