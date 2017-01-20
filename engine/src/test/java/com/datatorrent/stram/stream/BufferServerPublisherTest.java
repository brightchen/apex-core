package com.datatorrent.stram.stream;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.stram.engine.StreamContext;


public class BufferServerPublisherTest
{
  @Test
  public void test()
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
}
