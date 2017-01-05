package com.datatorrent.stram.codec;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import org.apache.apex.engine.serde.SerializationBuffer;

public class DefaultStatefulStreamCodecPerformanceTest
{
  private DefaultStatefulStreamCodec codec = new DefaultStatefulStreamCodec();

  private int numOfValues = 1000000;
  private String[] values = new String[numOfValues];
  private Random random = new Random();
  private int valueLen = 1000;
  private char[] chars;

  @Before
  public void initValues()
  {
    //init chars
    chars = new char[26*2 + 10];
    int i = 0;
    for(; i<26; ++i) {
      chars[i] = (char)('A' + i);
    }
    for(; i<52; ++i) {
      chars[i] = (char)('a' + i - 26);
    }
    for(; i<chars.length; ++i) {
      chars[i] = (char)('0' + i - 52);
    }

    char[] chars1 = new char[valueLen];
    for(i=0; i<values.length; ++i){
      for(int j=0; j<valueLen; ++j) {
        chars1[j] = chars[random.nextInt(chars.length)];
      }
      values[i] = new String(chars1);
    }
  }

  @Test
  public void testKryo()
  {
    long startTime = System.currentTimeMillis();
    for(int i=0; i<values.length; ++i) {
      codec.toDataStatePairOld(values[i]);
    }
    long spent = System.currentTimeMillis() - startTime;
    System.out.println("spent times for kryo: " + spent);
  }


  @Test
  public void testSpecific()
  {
    long startTime = System.currentTimeMillis();
    for(int i=0; i<values.length; ++i) {
      codec.toDataStatePairNew(values[i]);
    }
    long spent = System.currentTimeMillis() - startTime;

    System.out.println("spent times for specific: " + spent);
  }

  /**
   * why this test case stucked?
   */
  /***
  @Test
  public void testKryoWriteString()
  {
    Output output = new Output();
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 10; ++i) {
      output.setPosition(0);
      output.writeString(data);
    }
    output.close();
    System.out.println("spent times for kryo write string: " + (System.currentTimeMillis() - startTime));
  }
  */

  @Test
  public void testSpecificWriteString()
  {
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    long startTime = System.currentTimeMillis();
    int count = 0;
    for (int i = 0; i < values.length; ++i) {
      if(count++ > 1000) {
        output.reset();
      }
      output.writeString(values[i]);
    }
    System.out.println("spent times for specific write string: " + (System.currentTimeMillis() - startTime));
  }
}
