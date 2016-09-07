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

import org.quartz.spi.ClassLoadHelper;

import java.io.InputStream;
import java.net.URL;

/**
 * A class loader helper for Quartz that allows an extenal classloader to
 * provide some of the classloading.
 * 
 * <p>
 * This is to help with platforms like OSGi.
 * 
 * @author Keith M. Hughes
 */
public class InternalClassLoaderHelper implements ClassLoadHelper {

  /**
   * The provided classloader that provides additional classes.
   */
  private ClassLoader classLoader;

  /**
   * The initial provider of classes.
   */
  private ClassLoadHelper providedClassLoadHelper;

  /**
   * Construct a new helper.
   * 
   * @param classLoader
   *          the provided classloader that provides additional classes
   * @param providedClassLoadHelper
   *          the initial provider of classes
   */
  public InternalClassLoaderHelper(ClassLoader classLoader,
      ClassLoadHelper providedClassLoadHelper) {
    this.classLoader = classLoader;
    this.providedClassLoadHelper = providedClassLoadHelper;
  }

  @Override
  public void initialize() {
    // Nothing to do
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    Class<?> clazz = null;

    try {
      clazz = providedClassLoadHelper.loadClass(name);
    } catch (ClassNotFoundException e) {
      clazz = classLoader.loadClass(name);
    }

    return clazz;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Class<? extends T> loadClass(String name, Class<T> clazz)
      throws ClassNotFoundException {
    Class<? extends T> clazzRet = null;
    try {
      clazzRet = providedClassLoadHelper.loadClass(name, clazz);
    } catch (ClassNotFoundException e) {
      clazzRet = (Class<? extends T>) classLoader.loadClass(name);
    }

    return clazzRet;
  }

  @Override
  public URL getResource(String name) {
    URL url = providedClassLoadHelper.getResource(name);

    if (url == null) {
      url = classLoader.getResource(name);
    }

    return url;
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    InputStream stream = providedClassLoadHelper.getResourceAsStream(name);

    if (stream == null) {
      stream = classLoader.getResourceAsStream(name);
    }

    return stream;
  }

  @Override
  public ClassLoader getClassLoader() {
    // TODO Auto-generated method stub
    return null;
  }

}
