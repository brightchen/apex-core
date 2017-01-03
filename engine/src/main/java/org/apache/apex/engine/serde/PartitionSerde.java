package org.apache.apex.engine.serde;

import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.netlet.util.Slice;

/**
 * serialize partition before serialize object
 *
 * @param <T>
 */
public class PartitionSerde<T>
{
  public static PartitionSerde DEFAULT = new PartitionSerde();

  private Serde<T> objectSerde;

  private PartitionSerde()
  {
    objectSerde = GenericSerde.DEFAULT;
  }

  public Slice serialize(int partition, T object, SerializationBuffer output)
  {
    output.writeByte(MessageType.PAYLOAD_VALUE);
    output.writeByte((byte)partition);
    output.writeByte((byte)(partition >> 8));
    output.writeByte((byte)(partition >> 16));
    output.writeByte((byte)(partition >> 24));
    objectSerde.serialize(object, output);
    return output.toSlice();
  }
}
