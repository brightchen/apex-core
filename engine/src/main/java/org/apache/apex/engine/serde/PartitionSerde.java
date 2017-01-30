package org.apache.apex.engine.serde;

import com.esotericsoftware.kryo.Kryo;

import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.netlet.util.Slice;

/**
 * serialize partition before serialize object
 *
 * @param <T>
 */
public class PartitionSerde<T> extends Kryo
{

  public Slice serialize(int partition, T object, SerializationBuffer output)
  {
    Slice slice = output.reserve(5);
    int offset = slice.offset;
    byte[] buffer = slice.buffer;
    buffer[offset++] = MessageType.PAYLOAD_VALUE;
    buffer[offset++] = (byte)partition;
    buffer[offset++] = (byte)(partition >> 8);
    buffer[offset++] = (byte)(partition >> 16);
    buffer[offset++] = (byte)(partition >> 24);

    writeClassAndObject(output, object);
    Slice slice1 = output.toSlice();
    byte[] array = new byte[slice1.length];
    System.arraycopy(slice1.buffer, slice1.offset, array, 0, slice1.length);
    return new Slice(array);
  }
}
