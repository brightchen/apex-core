package com.datatorrent.bufferserver.server;

import java.io.IOException;

public interface ClientListenerExt
{
  /**
   * callback method for write data to socket
   */
  public void writeExt() throws IOException;
}
