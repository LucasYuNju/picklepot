/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.picklepot.serialization;

import com.intel.picklepot.column.Readers;
import com.intel.picklepot.column.Writers;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeBooleanField extends UnsafeField {
  /**
   * @param clazz  field type
   * @param offset field offset
   */
  public UnsafeBooleanField(Class clazz, long offset) {
    super(clazz, offset);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = new Writers.BooleanColumnWriter(picklePot.getOutput());
    }
    Boolean booleanVal;
    if(clazz == boolean.class) {
      booleanVal = Utils.unsafe().getBoolean(object, offset);
    }
    else {
      booleanVal = (Boolean) Utils.unsafe().getObject(object, offset);
    }
    writer.write(booleanVal);
  }

  @Override
  public void read(Object object) throws PicklePotException {
    if(reader == null) {
      reader = new Readers.BooleanColumnReader(picklePot.getInput());
    }
    Boolean intVal = (Boolean) reader.read();
    if(clazz == int.class) {
      Utils.unsafe().putBoolean(object, offset, intVal);
    }
    else {
      Utils.unsafe().putObject(object, offset, intVal);
    }
  }
}
