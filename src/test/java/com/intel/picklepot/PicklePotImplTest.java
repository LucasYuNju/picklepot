package com.intel.picklepot;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.metadata.ClassInfo;
import com.intel.picklepot.serialization.InstancePot;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.List;

public class PicklePotImplTest {

  @Test
  public void simpleTest() throws PicklePotException {
    PicklePotImpl<Pair> picklePot = new PicklePotImpl<Pair>();
    picklePot.initialize(Pair.class, null, null);
    Pair wc1 = new Pair("hello", 1);
    Pair wc2 = new Pair("world", 2);

    picklePot.add(wc1);
    picklePot.add(wc2);

    InstancePot<Pair> instancePot = picklePot.getInstancePot();
    ClassInfo<Pair> classInfo = instancePot.getClassInfo();
    List<?> wordList = instancePot.getFieldValues("word");
    List<?> countList = instancePot.getFieldValues("count");

    assertEquals(wordList.toString(), "[hello, world]");
    assertEquals(countList.toString(), "[1, 2]");
  }
}

