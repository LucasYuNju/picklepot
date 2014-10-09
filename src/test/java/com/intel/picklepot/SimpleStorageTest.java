package com.intel.picklepot;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.metadata.ClassInfo;
import com.intel.picklepot.metadata.FieldInfo;
import com.intel.picklepot.storage.DataInput;
import com.intel.picklepot.storage.DataOutput;
import com.intel.picklepot.storage.SimpleDataInput;
import com.intel.picklepot.storage.SimpleDataOutput;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.*;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Test SimpleDataOutput & SimpleDataInput
 */
public class SimpleStorageTest {

  @Test
  public void simpleTest() throws Exception {
    DumbPicklePot dumbPicklePot = new DumbPicklePot();

    File file = new File("test.ser");

    FileOutputStream fileOutputStream = new FileOutputStream(file);
    dumbPicklePot.initialize(Pair.class, new SimpleDataOutput(fileOutputStream), null);
    dumbPicklePot.add(new Pair("hello", 1));
    dumbPicklePot.add(new Pair("world", 2));
    dumbPicklePot.flush();

    FileInputStream fileInputStream = new FileInputStream(file);
    SimpleDataInput simpleDataInput = new SimpleDataInput();
    simpleDataInput.initialize(fileInputStream);
    simpleDataInput.readObjects();

    assertEquals(Pair.class, simpleDataInput.getClassInfo().getClassIns());
    assertEquals("helloworld", new String(simpleDataInput.getFieldsByte().get(0)));
    assertArrayEquals(dumbPicklePot.getCountStream().toByteArray(), simpleDataInput.getFieldsByte().get(1));

    file.delete();
  }
}

class DumbPicklePot implements PicklePot<Pair> {
  private ClassInfo<Pair> classInfo;
  private SimpleDataOutput output;
  ByteArrayOutputStream wordStream;
  ByteArrayOutputStream countStream;

  public ByteArrayOutputStream getWordStream() {
    return wordStream;
  }

  public ByteArrayOutputStream getCountStream() {
    return countStream;
  }

  @Override
  public void initialize(Class<Pair> className, DataOutput output, Map<String, String> configuration) {
    classInfo = new ClassInfo<Pair>(className);
    Field[] fields = classInfo.getClassIns().getDeclaredFields();
    for (Field field : fields) {
      FieldInfo fieldInfo = new FieldInfo(field.getName(), field.getType());
      classInfo.putFieldInfo(fieldInfo);
    }
    if (output instanceof SimpleDataOutput) {
      this.output = (SimpleDataOutput) output;
    }
    wordStream = new ByteArrayOutputStream();
    countStream = new ByteArrayOutputStream();
  }

  @Override
  public long add(Pair obj) throws PicklePotException {
    try {
      wordStream.write(obj.word.getBytes());
      countStream.write(obj.count);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return 0;
  }

  @Override
  public long add(Iterator<Pair> ite) throws PicklePotException {
    return 0;
  }

  @Override
  public Iterator<Pair> deserialize(DataInput input) {
    return null;
  }

  public void flush() throws IOException, PicklePotException {
    if (output != null) {
      List<byte[]> fieldsByte = new ArrayList<byte[]>();
      fieldsByte.add(wordStream.toByteArray());
      fieldsByte.add(countStream.toByteArray());
      output.writeObjects(classInfo, fieldsByte);
    }
  }
}
