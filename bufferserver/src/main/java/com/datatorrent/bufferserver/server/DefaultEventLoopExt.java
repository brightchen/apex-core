package com.datatorrent.bufferserver.server;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.Listener;
import com.datatorrent.netlet.Listener.ClientListener;
import com.datatorrent.netlet.Listener.ServerListener;

public class DefaultEventLoopExt extends DefaultEventLoop
{
  private static final Logger logger = LoggerFactory.getLogger(DefaultEventLoopExt.class);

  /**
   * @deprecated use factory method {@link #createEventLoop(String)}
   * @param id of the event loop
   */
  @Deprecated
  public DefaultEventLoopExt(String id) throws IOException
  {
    super(id);
  }

  public static DefaultEventLoopExt createEventLoop(final String id) throws IOException
  {
    @SuppressWarnings("deprecation")
    DefaultEventLoopExt el = new DefaultEventLoopExt(id);
    return el;
  }

  @Override
  @SuppressWarnings({"SleepWhileInLoop", "ConstantConditions"})
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
              }
              while (--size > 0);
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
            handleSelectedKeyExt(sk);
          }

          selectedKeys.clear();
        }
        while (alive);
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
    }
    while (alive);
    //logger.debug("Terminated {}", this);
  }

  protected final void handleSelectedKeyExt(final SelectionKey sk) throws IOException
  {
    Object attachment = null;
    switch (sk.readyOps()) {
      case SelectionKey.OP_ACCEPT:
        ServerSocketChannel ssc = (ServerSocketChannel)sk.channel();
        SocketChannel sc = ssc.accept();
        sc.configureBlocking(false);
        final ServerListener sl = (ServerListener)sk.attachment();
        final ClientListener l = sl.getClientConnection(sc, (ServerSocketChannel)sk.channel());
        register(sc, SelectionKey.OP_READ | SelectionKey.OP_WRITE, l);
        break;

      case SelectionKey.OP_CONNECT:
        if (((SocketChannel)sk.channel()).finishConnect()) {
          ((ClientListener)sk.attachment()).connected();
        }
        break;

      case SelectionKey.OP_READ:
        ((ClientListener)sk.attachment()).read();
        break;

      case SelectionKey.OP_WRITE:
        attachment = sk.attachment();
        if (attachment instanceof ClientListenerExt) {
          ((ClientListenerExt)attachment).writeExt();
        } else {
          ((ClientListener)attachment).write();
        }
        break;

      case SelectionKey.OP_READ | SelectionKey.OP_WRITE:
        ((ClientListener)sk.attachment()).read();
        attachment = sk.attachment();
        if (attachment instanceof ClientListenerExt) {
          ((ClientListenerExt)attachment).writeExt();
        } else {
          ((ClientListener)attachment).write();
        }
        break;

      case SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT:
      case SelectionKey.OP_READ | SelectionKey.OP_CONNECT:
      case SelectionKey.OP_READ | SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT:
        if (((SocketChannel)sk.channel()).finishConnect()) {
          ((ClientListener)sk.attachment()).connected();
          if (sk.isReadable()) {
            ((ClientListener)sk.attachment()).read();
          }
          if (sk.isWritable()) {
            attachment = sk.attachment();
            if (attachment instanceof ClientListenerExt) {
              ((ClientListenerExt)attachment).writeExt();
            } else {
              ((ClientListener)attachment).write();
            }
          }
        }
        break;

      default:
        logger.warn("!!!!!! not sure what interest this is {} !!!!!!", Integer.toBinaryString(sk.readyOps()));
        break;
    }

  }
}
