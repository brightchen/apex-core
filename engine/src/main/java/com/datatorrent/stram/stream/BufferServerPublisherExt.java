package com.datatorrent.stram.stream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.apex.engine.serde.BlockStream;
import org.apache.apex.engine.serde.MemReuseCodec;
import org.apache.apex.engine.serde.PartitionSerde;
import org.apache.apex.engine.serde.SerializationBuffer;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.EndStreamTuple;
import com.datatorrent.bufferserver.packet.EndWindowTuple;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.ResetWindowTuple;
import com.datatorrent.bufferserver.packet.WindowIdTuple;
import com.datatorrent.bufferserver.server.ClientListenerExt;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.codec.StatefulStreamCodec;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.tuple.Tuple;

import static java.lang.Thread.sleep;

public class BufferServerPublisherExt extends BufferServerPublisher implements ClientListenerExt
{
  private MemReuseCodec<Object> memReuseCodec;

  public BufferServerPublisherExt(String sourceId, int queueCapacity)
  {
    super(sourceId, queueCapacity);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setup(StreamContext context)
  {
    StreamCodec<?> codec = context.get(StreamContext.CODEC);
    if (codec == null) {
      statefulSerde = ((StatefulStreamCodec<Object>)StreamContext.CODEC.defaultValue).newInstance();
    } else if (codec instanceof StatefulStreamCodec) {
      statefulSerde = ((StatefulStreamCodec<Object>)codec).newInstance();
    } else if (codec instanceof MemReuseCodec) {
      /**
       * maybe need to consider combine with StatefulStreamCodec, but
       * StatefulStreamCodec used for register class with class id. it maybe
       * better to use another seperate attribute.
       */
      memReuseCodec = (MemReuseCodec<Object>)codec;
    } else {
      serde = (StreamCodec<Object>)codec;
    }
  }

  /**
   * serialize only, a dedicated thread response for write.
   * As serialize is cpu concentrate and write should be I/O concentrate. two thread should increase the performance
   * @param payload
   */
  private PartitionSerde partitionSerde = new PartitionSerde();
  private SerializationBuffer serializationBuffer = SerializationBuffer.READ_BUFFER;
  private int tupleCount = 0;
  @Override
  @SuppressWarnings({ "SleepWhileInLoop", "unchecked" })
  public void put(Object payload)
  {
    count++;
    byte[] array;
    Slice slice = null;
    if (payload instanceof Tuple) {
      final Tuple t = (Tuple)payload;

      switch (t.getType()) {
        case CHECKPOINT:
          if (statefulSerde != null) {
            statefulSerde.resetState();
          }
          array = WindowIdTuple.getSerializedTuple((int)t.getWindowId());
          array[0] = MessageType.CHECKPOINT_VALUE;
          break;

        case BEGIN_WINDOW:
          array = BeginWindowTuple.getSerializedTuple((int)t.getWindowId());
          break;

        case END_WINDOW:
          array = EndWindowTuple.getSerializedTuple((int)t.getWindowId());
          break;

        case END_STREAM:
          array = EndStreamTuple.getSerializedTuple((int)t.getWindowId());
          break;

        case RESET_WINDOW:
          com.datatorrent.stram.tuple.ResetWindowTuple rwt = (com.datatorrent.stram.tuple.ResetWindowTuple)t;
          array = ResetWindowTuple.getSerializedTuple(rwt.getBaseSeconds(), rwt.getIntervalMillis());
          break;

        default:
          throw new UnsupportedOperationException("this data type is not handled in the stream");
      }


      serializationBuffer.write(array);
      slice = serializationBuffer.toSlice();
      try {
        while (!write(slice.buffer, slice.offset, slice.length)) {
          sleep(5);
        }
        publishedByteCount.addAndGet(array.length);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }

    } else {
      if (serde != null) {
        int partition = serde.getPartition(payload);
        Slice f = serde.toByteArray(payload);
        serializationBuffer.writeByte(MessageType.PAYLOAD_VALUE);
        serializationBuffer.writeInt(partition);
        serializationBuffer.write(f.buffer, f.offset, f.length);
      } else if (memReuseCodec != null) {
        slice = partitionSerde.serialize(payload.hashCode(), payload, serializationBuffer);
      }
      if (slice != null) {
        this.blockWrite(slice);
        publishedByteCount.addAndGet(slice.length);
      }

      if (++tupleCount == 1000) {
        serializationBuffer.reset();
        tupleCount = 0;
      }
    }
  }

  @Override
  public boolean write(byte[] message, int offset, int size)
  {
    blockWrite(new Slice(message, offset, size));
    return true;
  }

  private static class BufferInfo
  {
    static final int SLICE_NUM = 1000;
    final SerializationBuffer buffer = new SerializationBuffer(new BlockStream());
    Slice[] slices = new Slice[SLICE_NUM];
    int sliceNum = 0;
  }

  private BufferInfo serializationBufferInfo = new BufferInfo();
  private int serializationSlicesIndex = -1;

  private BufferInfo socketBufferInfo = new BufferInfo();
  private int socketSlicesIndex = -1;

  private AtomicBoolean requestSwitch = new AtomicBoolean();

  private Exchanger<BufferInfo> exchanger = new Exchanger<>();

//  private final int SLICE_NUM = 1000;
//  private SerializationBuffer inputBuffer = new SerializationBuffer(new BlockStream());
//  private Slice[] inputSlices = new Slice[SLICE_NUM];
//
//  private SerializationBuffer outputBuffer = new SerializationBuffer(new BlockStream());
//  private Slice[] outputSlices = new Slice[SLICE_NUM];

  //set this flag when writer buffer emptied.
//  private AtomicBoolean switchImmediately = new AtomicBoolean(false);
  //set this flag when serialize buffer is full.

  //private Object serializationReady = new Object();
//  private Object writerReady = new Object();
//  private Object switching = new Object();


  /**
   * write until success.
   * @param slice
   */
  public void blockWrite(Slice slice)
  {
    if (slice == null) {
      throw new IllegalArgumentException("Input Slice should not null.");
    }
    try {
      if (++serializationSlicesIndex >= BufferInfo.SLICE_NUM) {
//        System.out.println("serializationBufferInfo full. going to exchange. sliceNum = " + serializationSlicesIndex);
        serializationBufferInfo.sliceNum = serializationSlicesIndex;
        serializationBufferInfo = exchanger.exchange(serializationBufferInfo);
//        System.out.println("exchanged.");
        serializationSlicesIndex = 0;
      }

      serializationBufferInfo.slices[serializationSlicesIndex] = slice;
      if (requestSwitch.get()) {
//        System.out.println("writer ask for exchange. going to exchange. sliceNum = " + (serializationSlicesIndex + 1));
        serializationBufferInfo.sliceNum = serializationSlicesIndex + 1;
        serializationBufferInfo = exchanger.exchange(serializationBufferInfo);
        serializationSlicesIndex = -1;
//        System.out.println("exchanged.");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * callback method to write to the socket
   */
  private ByteBuffer writeToSocketBuffer = null;

  /**
   *
   * @return Return true if can continue to send data.
   */
  protected boolean writeByteBufferToSocket() throws IOException
  {
    SocketChannel channel = (SocketChannel)key.channel();
    int lengthToSend = 0;

    //send remain data of writeToSocketBuffer
    if (writeToSocketBuffer != null) {
      lengthToSend = writeToSocketBuffer.remaining();
      while (lengthToSend > 0) {
        int sentLen = channel.write(writeToSocketBuffer);
        if (sentLen <= 0) {
          //the socket can't accept more data.
          return false;
        }
        lengthToSend -= sentLen;
      }
    }
    return true;
  }

  @Override
  public void writeExt() throws IOException
  {
    if (!writeByteBufferToSocket()) {
      return;
    }

    //send data from socket slices
    if (socketSlicesIndex < 0) {
      socketBufferInfo.buffer.reset();
      //exchange
      requestSwitch.set(true);
      try {
        socketBufferInfo = exchanger.exchange(socketBufferInfo);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      socketSlicesIndex = 0;
    }

    //merge and send
    byte[] currentBuffer = null;
    int offset = 0;
    int length = 0;
    for (; socketSlicesIndex < socketBufferInfo.sliceNum; ++socketSlicesIndex) {
      if (currentBuffer == null) {
        //initialize
        try {
        currentBuffer = socketBufferInfo.slices[socketSlicesIndex].buffer;
        }catch(Exception e) {
          throw new RuntimeException("");
        }
        offset = socketBufferInfo.slices[socketSlicesIndex].offset;
        length = socketBufferInfo.slices[socketSlicesIndex].length;
      } else if (currentBuffer == socketBufferInfo.slices[socketSlicesIndex].buffer) {
        //merge
        length += socketBufferInfo.slices[socketSlicesIndex].length;
      } else {
        //different block, send the data
        writeToSocketBuffer = ByteBuffer.wrap(currentBuffer, offset, length);
        if (!writeByteBufferToSocket()) {
          return;
        }

      }
    }

    //all slices are handled, ready from exchange
    socketSlicesIndex = -1;
  }
}
