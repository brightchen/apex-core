package org.apache.apex.engine.serde;

import com.esotericsoftware.kryo.Kryo;

import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.netlet.util.VarInt;

/**
 * serialize length, message type and partition before serialize object
 *
 * @param <T>
 */
public class PartitionSerde<T> extends Kryo
{

  /**
   * this is the sample implementation if you fixed length for length
   * @param partition
   * @param object
   * @param output
   * @return
   */
//  public Slice serialize(int partition, T object, SerializationBuffer output)
//  {
//    //for length, type and partition
//    Slice slice = output.reserve(9);
//
//    //type and partition
//    int offset = slice.offset + 4;
//    byte[] buffer = slice.buffer;
//    buffer[offset++] = MessageType.PAYLOAD_VALUE;
//    buffer[offset++] = (byte)partition;
//    buffer[offset++] = (byte)(partition >> 8);
//    buffer[offset++] = (byte)(partition >> 16);
//    buffer[offset++] = (byte)(partition >> 24);
//
//    writeClassAndObject(output, object);
//    Slice totalSlice = output.toSlice();
//    writeLength(totalSlice.length, totalSlice.buffer);
//    return totalSlice;
//  }

  /*
   * This format compatible apex current integer format
   */
  public static byte[] writeLength(int length, byte[] buffer)
  {
    for (int i = 0; i < 4; ++i) {
      if (i == 3) {
        buffer[i] = (byte)length;
        break;
      } else {
        buffer[i] = (byte)((length & 0x7F) | 0x80);
        length >>>= 7;
      }
    }
    return buffer;
  }

  public Slice serialize(int partition, T object, SerializationBuffer output)
  {
    //for length, type and partition
    Slice slice = output.reserve(5);

    //type and partition
    int offset = slice.offset;
    byte[] buffer = slice.buffer;
    buffer[offset++] = MessageType.PAYLOAD_VALUE;
    buffer[offset++] = (byte)partition;
    buffer[offset++] = (byte)(partition >> 8);
    buffer[offset++] = (byte)(partition >> 16);
    buffer[offset++] = (byte)(partition >> 24);

    writeClassAndObject(output, object);
    Slice totalSlice = output.toSlice();

    byte[] byteLength = new byte[4];
    int lenOfLen = VarInt.write(totalSlice.length, byteLength, 0);
    byte[] data = new byte[totalSlice.length + lenOfLen];
    System.arraycopy(byteLength, 0, data, 0, lenOfLen);
    System.arraycopy(totalSlice.buffer, totalSlice.offset, data, lenOfLen, totalSlice.length);

    return new Slice(data);
  }
}
