package com.datatorrent.stram.stream;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.engine.serde.PartitionSerde;
import org.apache.apex.engine.serde.SerializationBuffer;

import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.codec.CodecPerformanceTest;
import com.datatorrent.stram.engine.StreamContext;


public class BufferServerPublisherTest extends CodecPerformanceTest
{
  @Test
  public void testPutWith2Threads()
  {
    final String id = "1";
    BufferServerPublisher publisher = new BufferServerPublisher(id, 1000);
    publisher.setup(new StreamContext(id));
    for(int i=0; i<10000; ++i) {
      publisher.put_new_2threads(i);
    }
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertTrue("putCount = " + publisher.putCount + "; writtenCount = " + publisher.writtenCount, publisher.putCount == publisher.writtenCount);
  }

  @Test
  public void testWrite()
  {
    StringBuilder sb = new StringBuilder();
    for(int i=0; i<10; ++i) {
      sb.append("0123456789");
    }

    PartitionSerde serde = new PartitionSerde();
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    output.reset();
    Slice slice = serde.serialize(1, sb.toString(), output);

    final String id = "1";
    BufferServerPublisher publisher = new BufferServerPublisher(id, 1000);
    publisher.setup(new StreamContext(id));

    long startTime = System.currentTimeMillis();
    long count = 0;
    logRate(count);
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < values.length; ++i) {
        if(!publisher.write(slice.buffer, slice.offset, slice.length)) {
          continue;
        }
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
}
