package com.datatorrent.stram.codec;

import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.apex.engine.serde.PartitionSerde;
import org.apache.apex.engine.serde.SerializationBuffer;

import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.codec.StatefulStreamCodec.DataStatePair;

public class CodecPerformanceTest
{
  private DefaultStatefulStreamCodec codec = new DefaultStatefulStreamCodec();

  private int loop = 10000000;
  private int numOfValues = 1000;
  private String[] values = new String[numOfValues];
  private Random random = new Random();
  private int valueLen = 10000;
  private char[] chars;

  @Before
  public void initValues()
  {
    //init chars
    chars = new char[26 * 2 + 10];
    int i = 0;
    for (; i < 26; ++i) {
      chars[i] = (char)('A' + i);
    }
    for (; i < 52; ++i) {
      chars[i] = (char)('a' + i - 26);
    }
    for (; i < chars.length; ++i) {
      chars[i] = (char)('0' + i - 52);
    }

    char[] chars1 = new char[valueLen];
    for (i = 0; i < values.length; ++i) {
      for (int j = 0; j < valueLen; ++j) {
        chars1[j] = chars[random.nextInt(chars.length)];
      }
      values[i] = new String(chars1);
    }
    System.out.println("==================initted==================");
  }

  @Test
  public void testDataStateNewFunctional()
  {
    for (int i = 0; i < Math.min(values.length, 10000); ++i) {
      DataStatePair dp1 = codec.toDataStatePairOld(values[i]);
      DataStatePair dp2 = codec.toDataStatePairNew(values[i]);
      Assert.assertTrue("Not equal: " + i, equals(dp1, dp2));
    }
  }

  protected boolean equals(DataStatePair dp1, DataStatePair dp2)
  {
    if (dp1 == null || dp2 == null) {
      return dp1 == dp2;
    }
    if (!equals(dp1.data, dp2.data)) {
      return false;
    }
    return equals(dp1.state, dp2.state);
  }

  protected boolean equals(Slice slice1, Slice slice2)
  {
    if (slice1 == null || slice2 == null) {
      return slice1 == slice2;
    }
    if (slice1.buffer == null || slice2.buffer == null) {
      return slice1.buffer == slice2.buffer;
    }
    if (slice1.length != slice2.length) {
      return false;
    }
    for (int i = 0; i < slice1.length; ++i) {
      if (slice1.buffer[slice1.offset + i] != slice2.buffer[slice2.offset + i]) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testKryo()
  {
    long startTime = System.currentTimeMillis();
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < values.length; ++i) {
        codec.toDataStatePairOld(values[i]);
      }
    }
    long spent = System.currentTimeMillis() - startTime;
    System.out.println("spent times for kryo: " + spent);
  }

  @Test
  public void testSpecific()
  {
    long startTime = System.currentTimeMillis();
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < values.length; ++i) {
        codec.toDataStatePairNew(values[i]);
      }
    }
    long spent = System.currentTimeMillis() - startTime;

    System.out.println("spent times for specific: " + spent);
  }

  /**
   * why this test case stucked?
   */

  @Test
  public void testKryoWriteString()
  {
    Output output = new Output(10000);
    long startTime = System.currentTimeMillis();
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < values.length; ++i) {
        output.setPosition(0);
        output.writeString(values[i]);
      }
    }
    output.close();
    System.out.println("spent times for kryo write string: " + (System.currentTimeMillis() - startTime));
  }

  @Test
  public void testSpecificWriteString()
  {
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    long startTime = System.currentTimeMillis();
    int count = 0;
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < values.length; ++i) {
        if (count++ > 1000) {
          output.reset();
          count = 0;
        }
        output.writeString(values[i]);
      }
    }
    System.out.println("spent times for specific write string: " + (System.currentTimeMillis() - startTime));
  }

  @Test
  public void testPartitionSerdeFunctional()
  {
    PartitionSerde serde = new PartitionSerde();
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    output.reset();

    for (int i = 0; i < Math.min(values.length, 1000); i++) {
      int partition = codec.getPartition(values[i]);

      @SuppressWarnings("unchecked")
      DataStatePair dsp = codec.toDataStatePairOld(values[i]);
      byte[] array = PayloadTuple.getSerializedTuple(partition, dsp.data);

      Slice slice = serde.serialize(partition, values[i], output);
      byte[] array1 = new byte[slice.length];
      System.arraycopy(slice.buffer, slice.offset, array1, 0, array1.length);
      Assert.assertArrayEquals(array, array1);
    }
  }

  private final int maxIntValue = 10000;
  @Test
  public void testPartitionSerdeForInt()
  {
    PartitionSerde serde = new PartitionSerde();
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    output.reset();

    long startTime = System.currentTimeMillis();
    int count = 0;
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < maxIntValue; ++i) {
        if (count++ > 1000) {
          output.reset();
          count = 0;
        }
        serde.serialize(i, i, output);
      }
    }
    System.out.println("spent times for PartitionSerde for int: " + (System.currentTimeMillis() - startTime));
  }

  @Test
  public void testDataStatePairForInt()
  {
    long startTime = System.currentTimeMillis();
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < maxIntValue; ++i) {
        DataStatePair dsp = codec.toDataStatePairOld(i);
        PayloadTuple.getSerializedTuple(i, dsp.data);
      }
    }
    System.out.println("spent times for DataState for int: " + (System.currentTimeMillis() - startTime));
  }

  private final int logPeriod = 300000;
  @Test
  public void testPartitionSerdeForString()
  {
    PartitionSerde serde = new PartitionSerde();
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    output.reset();

    long startTime = System.currentTimeMillis();
    long count = 0;
    logRate(count);
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < values.length; ++i) {
        serde.serialize(i, values[i], output);
        if (++count % logPeriod == 0) {
          logRate(count);
        }
        if (count % 1000 == 0) {
          output.reset();
        }
      }
    }
    System.out.println("spent times for PartitionSerde for string: " + (System.currentTimeMillis() - startTime));
  }

  @Test
  public void testDataStatePairForString()
  {
    long startTime = System.currentTimeMillis();
    long count = 0;
    logRate(count);
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < values.length; ++i) {
        DataStatePair dsp = codec.toDataStatePairOld(values[i]);
        PayloadTuple.getSerializedTuple(i, dsp.data);
        if (++count % logPeriod == 0) {
          logRate(count);
        }
      }
    }
    System.out.println("spent times for DataState for string: " + (System.currentTimeMillis() - startTime));
  }

  static class SimpleTuple
  {
    SimpleTuple(int age, String name)
    {
      this.age = age;
      this.name = name;
    }

    int age;
    String name;
  }

  private SimpleTuple[] tuples;
  protected void initTuples()
  {
    if (tuples != null) {
      return;
    }
    tuples = new SimpleTuple[numOfValues];
    for (int i = 0; i < tuples.length; ++i) {
      tuples[i] = new SimpleTuple(i % 100, values[i]);
    }
  }

  @Test
  public void testPartitionSerdeForSimpleTuple()
  {
    initTuples();
    PartitionSerde serde = new PartitionSerde();
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    output.reset();

    long startTime = System.currentTimeMillis();
    int count = 0;
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < tuples.length; ++i) {
        if (count++ > 1000) {
          output.reset();
          count = 0;
        }
        serde.serialize(i, tuples[i], output);
      }
    }
    System.out.println("spent times for PartitionSerde for SimpleTuples: " + (System.currentTimeMillis() - startTime));
  }

  @Test
  public void testDataStatePairForSimpleTuple()
  {
    initTuples();
    long startTime = System.currentTimeMillis();
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < tuples.length; ++i) {
        DataStatePair dsp = codec.toDataStatePairOld(tuples[i]);
        PayloadTuple.getSerializedTuple(i, dsp.data);
      }
    }
    System.out.println("spent times for DataState for SimpleTuples: " + (System.currentTimeMillis() - startTime));
  }

  @Test
  public void testReserve()
  {
    Random random = new Random();
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    for (int i = 0; i < 1000; ++i) {
      for (int j = 0; j < 1000000000; ++j) {
        output.reserve(random.nextInt(1000) + 1);
        if (j % 10000 == 0) {
          output.reset();
        }
      }
    }
  }

  private long beginTime = 0;
  private long lastLogTime = 0;
  private long lastCount = 0;
  protected void logRate(long count)
  {
    long now = System.currentTimeMillis();
    if(lastLogTime == 0) {
      lastLogTime = now;
    }
    if(beginTime == 0) {
      beginTime = now;
    }
    if(now > lastLogTime) {
      System.out.println("Time: " + (now - lastLogTime) + "; period rate: " + (count - lastCount) / (now - lastLogTime) + "; total rate: " + count / (now - beginTime));
      lastLogTime = now;
      lastCount = count;
    }
  }
}
