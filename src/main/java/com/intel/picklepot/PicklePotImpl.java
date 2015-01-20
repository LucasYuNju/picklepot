package com.intel.picklepot;

import com.intel.picklepot.columnar.*;
import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.metadata.Block;
import com.intel.picklepot.metadata.FieldInfo;
import com.intel.picklepot.serialization.InstancePot;
import com.intel.picklepot.storage.DataInput;
import com.intel.picklepot.storage.DataOutput;
import com.intel.picklepot.storage.SimpleDataInput;
import com.intel.picklepot.storage.SimpleDataOutput;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * PicklePotImpl is not thread-safe, so do not use it in multi-threads.
 * @param <T>
 */
public class PicklePotImpl<T> implements PicklePot<T>{
  private Class<T> className;
  private DataOutput dataOutput;
  private Map<String, String> configuration;
  private InstancePot<T> instancePot;
  private volatile long count = 0;

  @Override
  public void initialize(Class<T> className, DataOutput output, Map<String, String> configuration) {
    this.className = className;
    this.dataOutput = output;
    this.configuration = configuration;
    this.instancePot = new InstancePot<T>(this.className);
  }

  @Override
  public long add(T obj) throws PicklePotException {
    if (obj == null) {
      return count;
    }
    this.instancePot.addObjectValue(obj);
    return ++count;
  }

  @Override
  public long add(Iterator<T> ite) throws PicklePotException {
    if (ite == null) {
      return count;
    }

    StopWatch.start();
    while(ite.hasNext()) {
      T obj = ite.next();
      this.instancePot.addObjectValue(obj);
      count++;
    }
    StopWatch.stop("InstancePot.addObjectValue");

    return count;
  }

  @Override
  public Iterator<T> deserialize(DataInput input) throws PicklePotException {
    SimpleDataInput dataInput = (SimpleDataInput) input;
    dataInput.readObjects();
    List<ColumnReader> readers = new ArrayList<ColumnReader>();
    Iterator<FieldInfo> fieldInfos = dataInput.getClassInfo().getFieldInfos().values().iterator();
    while (fieldInfos.hasNext()) {
      Block dataBlock = dataInput.readBlock();
      Block dictBlock = dataBlock.getEncoding().usesDictionary() ? dataInput.readBlock() : null;
      try {
        readers.add(new ColumnReader(dataBlock, dictBlock, fieldInfos.next().getFieldClass()));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    logReaders(readers);

    Class<T> clazz = dataInput.getClassInfo().getClassIns();
    Field[] fields = clazz.getDeclaredFields();
    for(Field field : fields) {
      field.setAccessible(true);
    }
    Objenesis objenesis = new ObjenesisStd();
    ObjectInstantiator instantiator = objenesis.getInstantiatorOf(clazz);
    List<T> result = new LinkedList<T>();
    if(!dataInput.getClassInfo().isSerializedWithJava()) {
      while(!readers.isEmpty() && readers.get(0).hasNext()) {
        try {
          T object = (T) instantiator.newInstance();
          for (int i = 0; i < fields.length; i++) {
            fields[i].set(object, readers.get(i).read());
          }
          result.add(object);
        } catch (IllegalAccessException e) {
          throw new PicklePotException(e);
        }
      }
    }
    else {
      ColumnReader soleReader = readers.get(0);
      while(soleReader.hasNext()) {
        result.add((T) soleReader.read());
      }
    }
    return result.iterator();
  }

  private void logReaders(List<ColumnReader> readers) {
    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/picklepot_encoding", true));
      writer.newLine();
      for(ColumnReader reader : readers) {
        writer.write("Encoding:" + reader.getEncoding().toString() + " Class:" + reader.getColumnClass());
        writer.newLine();
      }
      writer.flush();
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * do the compression work and flush to DataOutput.
   */
  public void flush() throws PicklePotException {
    SimpleDataOutput dataOutput = (SimpleDataOutput) this.dataOutput;
    try {
      dataOutput.initialize();
    } catch (IOException e) {
      e.printStackTrace();
    }
    dataOutput.writeClassInfo(instancePot.getClassInfo());
    Iterator<FieldInfo> fieldInfos = instancePot.getClassInfo().getFieldInfos().values().iterator();

    PicklePotProperties properties = new PicklePotProperties(true, 10 * 1024 * 1024, 1024 * 1024);
    while(fieldInfos.hasNext()) {
      FieldInfo curFieldInfo = fieldInfos.next();
      ColumnWriter columnWriter = properties.getColumnWriter(curFieldInfo.getFieldClass(), dataOutput);
      List<?> list = instancePot.getFieldValues(curFieldInfo.getFieldName());
      Iterator iterator = list.iterator();
      while(iterator.hasNext()) {
        columnWriter.write(iterator.next());
      }
      columnWriter.writeToBlock();
    }
  }

  /**
   * current implementation do not allow multiple flush. PicklePotImpl flush only once when it is closed
   */
  public void close() throws PicklePotException{
//    SimpleDataOutput dataOutput = (SimpleDataOutput) this.dataOutput;
    dataOutput.close();
  }

  // just used for unit test.
  public InstancePot getInstancePot() {
    return instancePot;
  }
}
