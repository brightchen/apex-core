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

  private int loop = 10;
  private int numOfValues = 100000;
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
  public void testReserve()
  {
    Random random = new Random();
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    for (int i = 0; i < 0; ++i) {
      for (int j = 0; j < 1000000000; ++j) {
        output.reserve(random.nextInt(1000) + 1);
        if (j % 10000 == 0) {
          output.reset();
        }
      }
    }
  }

  @Test
  public void testPartitionSerdeFunctional()
  {
    PartitionSerde serde = PartitionSerde.DEFAULT;
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

  @Test
  public void testPartitionSerde()
  {
    PartitionSerde serde = PartitionSerde.DEFAULT;
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    output.reset();

    long startTime = System.currentTimeMillis();
    int count = 0;
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < values.length; ++i) {
        if (count++ > 1000) {
          output.reset();
          count = 0;
        }
        serde.serialize(i, values[i], output);
      }
    }
    System.out.println("spent times for PartitionSerde: " + (System.currentTimeMillis() - startTime));
  }

  @Test
  public void testDataStatePair()
  {
    long startTime = System.currentTimeMillis();
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < values.length; ++i) {
        DataStatePair dsp = codec.toDataStatePairOld(values[i]);
        PayloadTuple.getSerializedTuple(i, dsp.data);
      }
    }
    System.out.println("spent times for DataState: " + (System.currentTimeMillis() - startTime));
  }
}
