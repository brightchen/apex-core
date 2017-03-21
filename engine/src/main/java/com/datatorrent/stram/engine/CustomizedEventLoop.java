/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.engine;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.Listener;

public class CustomizedEventLoop extends DefaultEventLoop   //OptimizedEventLoop
{
  public CustomizedEventLoop(String id) throws IOException
  {
    super(id);
  }

  @Override
  protected void runEventLoop()
  {
    //logger.debug("Starting {}", this);
    final Iterator<SelectionKey> EMPTY_ITERATOR = new Iterator<SelectionKey>()
    {

      @Override
      public boolean hasNext()
      {
        return false;
      }

      @Override
      public SelectionKey next()
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

    };

    final Set<SelectionKey> EMPTY_SET = new Set<SelectionKey>()
    {
      @Override
      public int size()
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public boolean isEmpty()
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public boolean contains(Object o)
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public Iterator<SelectionKey> iterator()
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public Object[] toArray()
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public <T> T[] toArray(T[] a)
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public boolean add(SelectionKey e)
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public boolean remove(Object o)
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public boolean containsAll(Collection<?> c)
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public boolean addAll(Collection<? extends SelectionKey> c)
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public boolean retainAll(Collection<?> c)
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public boolean removeAll(Collection<?> c)
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public void clear()
      {
      }

    };

    SelectionKey sk = null;
    Set<SelectionKey> selectedKeys = EMPTY_SET;
    Iterator<SelectionKey> iterator = EMPTY_ITERATOR;

    do {
      try {
        do {
          if (!iterator.hasNext()) {
            int size = tasks.size();
            if (size > 0) {
              Runnable task;
              do {
                task = tasks.poll();
                if (task == null) {
                  break;
                }
                task.run();
              } while (--size > 0);

              size = selector.selectNow();
            } else {
              size = selector.select(100);
            }

            if (size > 0) {
              selectedKeys = selector.selectedKeys();
              iterator = selectedKeys.iterator();
            } else {
              iterator = EMPTY_ITERATOR;
            }
          }

          while (iterator.hasNext()) {
            if (!(sk = iterator.next()).isValid()) {
              continue;
            }
            customizedHandleSelectedKey(sk);
          }

          selectedKeys.clear();
        } while (alive);
      } catch (Exception ex) {
        if (sk == null) {
          logger.warn("Unexpected exception not related to SelectionKey", ex);
        } else {
          logger.warn("Exception on unregistered SelectionKey {}", sk, ex);
          Listener l = (Listener)sk.attachment();
          if (l != null) {
            l.handleException(ex, this);
          }
        }
      }
    } while (alive);
    //logger.debug("Terminated {}", this);
  }

  private void customizedHandleSelectedKey(final SelectionKey sk) throws IOException
  {
    handleSelectedKey(sk);
  }

//  public void handleWorkSocket(final NioSocketChannel ch)
//  {
//    EventLoopGroup group = new NioEventLoopGroup();
//    try {
//      Bootstrap b = new Bootstrap();
//      b.group(group);
//
//      //ch.pipeline().addFirst(new MyEchoHandler()).addFirst(new StringDecoder()).addFirst(new StringEncoder());
//      io.netty.channel.EventLoop eventLoop = group.next();
//      ch.unsafe().register(eventLoop, new DefaultChannelPromise(ch, eventLoop));
//
//      if (!ch.isRegistered()) {
//        logger.warn("Channel not registered yet.");
//      }
//
//      //ch.closeFuture().sync();
//
//    } finally {
//      group.shutdownGracefully();
//    }
//  }

  private static final Logger logger = LoggerFactory.getLogger(CustomizedEventLoop.class);
}
