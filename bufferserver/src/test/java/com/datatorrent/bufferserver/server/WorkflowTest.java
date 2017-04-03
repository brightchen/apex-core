package com.datatorrent.bufferserver.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.EndWindowTuple;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.bufferserver.support.Publisher;
import com.datatorrent.bufferserver.support.Subscriber;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.util.Slice;

public class WorkflowTest
{
  private static byte[] content = "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890".getBytes();

  public static class TestSubscriber extends Subscriber
  {
    protected volatile int bytesCount = 0;

    public TestSubscriber(String id)
    {
      super(id);
    }


    private int lastBufferPosition = 0;
    @Override
    public void read(final int len)
    {
      ByteBuffer byteBuffer = buffer();
      int size = byteBuffer.position()-lastBufferPosition;
      byte[] bytes = new byte[size];

      System.arraycopy(byteBuffer.array(), lastBufferPosition, bytes, 0, size);
      logger.info("current read len: {}; size: {}; message: {}", len, size, new Slice(bytes));
      lastBufferPosition = byteBuffer.position();
      super.read(len);
      bytesCount += len;
//      logger.info("bytesCount: {}", bytesCount);
    }

    /**
     * The super onMessage didn't handle the case when messages are merged.
     * This implementation handle the case of messages are merged, but don't handle the case of message divided.
     */
    private byte[] buffer = new byte[20480];
    private int size = 0;
    @Override
    public void onMessage(byte[] newBuffer, int newOffset, int newSize)
    {
      if(newBuffer != null && newSize > 0) {
        logger.info("new bytes: {}", new Slice(newBuffer, newOffset, newSize));
        System.arraycopy(newBuffer, newOffset, buffer, size, newSize);
        size += newSize;
      }
      logger.info("tupleCount: {}; total bytes: {}", tupleCount.get(), new Slice(buffer, 0, size));
      Tuple tuple = null;
      try {
        tuple = Tuple.getTuple(buffer, 0, size);
      } catch (Exception e) {
        //wait for the leftover bytes
        if(size > 20) {
          //the bytes should be long enough, it should be invalid message
          Assert.assertFalse("Probably invalid message: " +  new Slice(buffer, 0, size), true);
        }
        return;
      }
      tupleCount.incrementAndGet();
      int windowId;
      int moveBytes = 0;
      switch (tuple.getType()) {
        case BEGIN_WINDOW:
          windowId = tuple.getWindowId();
          //beginWindow(windowId);
          moveBytes = getSize(windowId) + 1; //VarInt.getSize(windowId) + 1;
          break;

        case END_WINDOW:
          windowId = tuple.getWindowId();
          //endWindow(windowId);
          moveBytes = getSize(windowId) + 1; //VarInt.getSize(windowId) + 1;
          break;

        case RESET_WINDOW:
          int baseSecond = tuple.getBaseSeconds();
          int windowWidth = tuple.getWindowWidth();
          //resetWindow(baseSecond, windowWidth);
          moveBytes = getSize(baseSecond) + getSize(windowWidth) + 1;  //VarInt.getSize(baseSecond) + VarInt.getSize(windowWidth) + 1;
          break;

        case PAYLOAD:
          moveBytes = WorkflowTest.content.length + 5;
          break;

        case END_STREAM:
          //TODO: the buffer server suppose avoid send this message; handle for test
          moveBytes = 3;
          break;

        default:
          Assert.assertFalse("Unexpected tuple type: " + tuple.getType() + "; tupleCount: " + tupleCount.get() + "; data: " + new Slice(buffer, 0, size), true);
          break;
      }

      if (size > moveBytes) {
        size -= moveBytes;
        System.arraycopy(buffer, moveBytes, buffer, 0, size);
        onMessage(null, 0, 0);
      } else {
        //clean
        size = 0;
      }
    }

    //VarInt.getSize() incorrect? for example, input 64, return is 1
    private int getSize(int i) {
      if(i < 64) {
        return 1;
      }
      if(i > 8191) {
        return 3;
      }
      return 2;
    }
  }

  @Test(timeout = 600000)
  public void simpleTest() throws InterruptedException, IOException
  {
    DefaultEventLoop eventloopServer;
    DefaultEventLoop eventloopClient;

    try {
      eventloopServer = DefaultEventLoop.createEventLoop("server");
      eventloopClient = DefaultEventLoop.createEventLoop("client");
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    eventloopServer.start();
    eventloopClient.start();
    logger.info("eventloopClient: {}; eventloopServer: {}", System.identityHashCode(eventloopClient),
        System.identityHashCode(eventloopServer));

    InetSocketAddress address;
    Server instance = new Server(0, 4096, 8);
    address = instance.run(eventloopServer);

    final String subscriberId = "SubscriberClient";
    final String publisherId = "PublisherClient";

    TestSubscriber bss = new TestSubscriber(subscriberId);
    eventloopClient.connect(address, bss);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", publisherId, 0, null, 0L, 0);

    Publisher bsp = new Publisher(publisherId);
    eventloopClient.connect(address, bsp);
    bsp.activate(null, 0L);

    final int loops = 100;
    int windowId = 1;
    byte[] contentMessage = PayloadTuple.getSerializedTuple(1, content.length);
    System.arraycopy(content, 0, contentMessage, 5, content.length);
    for (int i = 0; i < loops; ++i, ++windowId) {
      byte[] msg = BeginWindowTuple.getSerializedTuple(windowId);
      bsp.write(msg);

      //msg = PayloadTuple.getSerializedTuple(0, 0);
      //msg = PayloadTuple.getSerializedTuple(1, content.length);

      bsp.write(contentMessage);

      msg = EndWindowTuple.getSerializedTuple(windowId);
      bsp.write(msg);
    }

    //each loop has beginWindow, message and endWindow, totally 9 bytes;
    //when loops larger or equal 64, the windowId become two bytes; and each loop is 11 bytes
    //final int expectedBytes = loops < 64 ? loops * 12 : (loops - 63) * 14 + 63 * 12;
    final int expectedTupleCount = loops * 3;
    final int spinCount = 60000;
    for (int i = 0; i < spinCount; ++i) {
      if (bss.tupleCount.get() >= expectedTupleCount) {
        break;
      }
      Thread.sleep(10);
    }

    //logger.info("bytesCount: {}, expected bytes count {}; tupleCount: {}, expected tuple count {}", bss.bytesCount, expectedBytes, bss.tupleCount.get(), expectedTupleCount);
    //Assert.assertTrue("expectedBytes = " + expectedBytes + "; actual = " + bss.bytesCount, bss.bytesCount == expectedBytes);
    Assert.assertTrue("expectedTupleCount = " + expectedTupleCount + "; actual = " + bss.tupleCount.get(), bss.tupleCount.get() == expectedTupleCount);

    eventloopClient.disconnect(bss);
    eventloopClient.disconnect(bsp);

    eventloopServer.stop(instance);
    eventloopServer.stop();
    eventloopClient.stop();
  }

  private static final Logger logger = LoggerFactory.getLogger(WorkflowTest.class);
}
