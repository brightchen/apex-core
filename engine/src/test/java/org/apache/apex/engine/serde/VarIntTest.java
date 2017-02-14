package org.apache.apex.engine.serde;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.bufferserver.util.VarInt;

public class VarIntTest
{
  @Test
  public void testVarInt()
  {
    int[] values = new int[] { 0, 1, 0x7f, 0xff, 0x100, 0x7fff, 0xffff, 0x10000 };

    for (int value : values) {
      byte[] buffer = new byte[4];
      VarInt.write(value, buffer, 0);
      VarInt.MutableInt offsetInd = new VarInt.MutableInt();
      int readValue = VarInt.read(buffer, 0, 4, offsetInd);
      Assert.assertTrue(value == readValue);
    }
  }

  @Test
  public void testFixedLengthInt()
  {
    int[] values = new int[] { 0, 1, 0x7f, 0xff, 0x100, 0x7fff, 0xffff, 0x10000 };

    for (int value : values) {
      byte[] buffer = writeInt(value);
      VarInt.MutableInt offsetInd = new VarInt.MutableInt();
      int readValue = VarInt.read(buffer, 0, 4, offsetInd);
      Assert.assertTrue(value == readValue);
      Assert.assertTrue("length is: " + offsetInd.integer, offsetInd.integer == 4);
    }
  }

  public byte[] writeInt(int value)
  {
    byte[] buffer = new byte[4];
    for (int i = 0; i < 4; ++i) {
      if (i == 3) {
        buffer[i] = (byte)value;
        break;
      } else {
        buffer[i] = (byte)((value & 0x7F) | 0x80);
        value >>>= 7;
      }
    }
    return buffer;
  }
}
