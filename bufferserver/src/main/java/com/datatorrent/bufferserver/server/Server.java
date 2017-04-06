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
package com.datatorrent.bufferserver.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import com.datatorrent.bufferserver.internal.DataList;
import com.datatorrent.bufferserver.internal.FastDataList;
import com.datatorrent.bufferserver.internal.LogicalNode;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.packet.PublishRequestTuple;
import com.datatorrent.bufferserver.packet.PurgeRequestTuple;
import com.datatorrent.bufferserver.packet.ResetRequestTuple;
import com.datatorrent.bufferserver.packet.SubscribeRequestTuple;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.bufferserver.storage.Storage;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.Listener.ServerListener;
import com.datatorrent.netlet.WriteOnlyLengthPrependerClient;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.netlet.util.VarInt;





/**
 * The buffer server application<p>
 * <br>
 *
 * @since 0.3.2
 */
public class Server implements ServerListener
{
  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024 * 1024;
  public static final int DEFAULT_NUMBER_OF_CACHED_BLOCKS = 8;
  private final int port;
  private String identity;
  private Storage storage;
  private EventLoop eventloop;
  private InetSocketAddress address;
  private final ExecutorService serverHelperExecutor;
  private final ExecutorService storageHelperExecutor;

  private byte[] authToken;

  private static final boolean BACK_PRESSURE_ENABLED = !Boolean.getBoolean("org.apache.apex.bufferserver.backpressure.disable");

  /**
   * @param port - port number to bind to or 0 to auto select a free port
   */
  public Server(int port)
  {
    this(port, DEFAULT_BUFFER_SIZE, DEFAULT_NUMBER_OF_CACHED_BLOCKS);
  }

  public Server(int port, int blocksize, int numberOfCacheBlocks)
  {
    this.port = port;
    this.blockSize = blocksize;
    this.numberOfCacheBlocks = numberOfCacheBlocks;
    serverHelperExecutor = Executors.newSingleThreadExecutor(new NameableThreadFactory("ServerHelper"));
    final ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(numberOfCacheBlocks);
    final NameableThreadFactory threadFactory = new NameableThreadFactory("StorageHelper");
    storageHelperExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, workQueue, threadFactory,
        new ThreadPoolExecutor.CallerRunsPolicy());
  }

  public void setSpoolStorage(Storage storage)
  {
    this.storage = storage;
  }

  @Override
  public synchronized void registered(SelectionKey key)
  {
    ServerSocketChannel channel = (ServerSocketChannel)key.channel();
    address = (InetSocketAddress)channel.socket().getLocalSocketAddress();
    logger.info("Server started listening at {}", address);
    notifyAll();
  }

  @Override
  public void unregistered(SelectionKey key)
  {
    for (LogicalNode ln : subscriberGroups.values()) {
      ln.boot();
    }
    /*
     * There may be unregister tasks scheduled to run on the event loop that use serverHelperExecutor.
     */
    eventloop.submit(new Runnable()
    {
      @Override
      public void run()
      {
        serverHelperExecutor.shutdown();
        storageHelperExecutor.shutdown();
        try {
          serverHelperExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
          logger.debug("Executor Termination", ex);
        }
        logger.info("Server stopped listening at {}", address);
      }
    });
  }

  public synchronized InetSocketAddress run(EventLoop eventloop)
  {
    eventloop.start(null, port, this);
    while (address == null) {
      try {
        wait(20);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }

    this.eventloop = eventloop;
    return address;
  }

  public void setAuthToken(byte[] authToken)
  {
    this.authToken = authToken;
  }

  /**
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception
  {
    int port;
    if (args.length > 0) {
      port = Integer.parseInt(args[0]);
    } else {
      port = 0;
    }

    DefaultEventLoop eventloop = DefaultEventLoop.createEventLoop("alone");
    eventloop.start(null, port, new Server(port));
    new Thread(eventloop).start();
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + '@' + Integer.toHexString(hashCode()) + "{address=" + address + "}";
  }

  private final ConcurrentHashMap<String, DataList> publisherBuffers = new ConcurrentHashMap<>(1, 0.75f, 1);
  private final ConcurrentHashMap<String, LogicalNode> subscriberGroups = new ConcurrentHashMap<String, LogicalNode>();
  private final ConcurrentHashMap<String, AbstractLengthPrependerClient> publisherChannels = new ConcurrentHashMap<>();
  private final int blockSize;
  private final int numberOfCacheBlocks;

  private void handlePurgeRequest(PurgeRequestTuple request, final AbstractLengthPrependerClient ctx) throws IOException
  {
    DataList dl;
    dl = publisherBuffers.get(request.getIdentifier());

    byte[] message;
    if (dl == null) {
      message = ("Invalid identifier '" + request.getIdentifier() + "'").getBytes();
    } else {
      dl.purge((long)request.getBaseSeconds() << 32 | request.getWindowId());
      message = ("Request sent for processing: " + request).getBytes();
    }

    final byte[] tuple = PayloadTuple.getSerializedTuple(0, message.length);
    System.arraycopy(message, 0, tuple, tuple.length - message.length, message.length);
    if (ctx.write(tuple)) {
      ctx.write();
    } else {
      logger.error("Failed to deliver purge ack message. {} send buffers are full.", ctx);
      throw new RuntimeException("Failed to deliver purge ack message. " + ctx + "send buffers are full.");
    }
  }

  public void purge(long windowId)
  {
    for (DataList dataList: publisherBuffers.values()) {
      dataList.purge(windowId);
    }
  }

  private void handleResetRequest(ResetRequestTuple request, final AbstractLengthPrependerClient ctx) throws IOException
  {
    DataList dl;
    dl = publisherBuffers.remove(request.getIdentifier());

    byte[] message;
    if (dl == null) {
      message = ("Invalid identifier '" + request.getIdentifier() + "'").getBytes();
    } else {
      AbstractLengthPrependerClient channel = publisherChannels.remove(request.getIdentifier());
      if (channel != null) {
        eventloop.disconnect(channel);
      }
      dl.reset();
      message = ("Request sent for processing: " + request).getBytes();
    }

    final byte[] tuple = PayloadTuple.getSerializedTuple(0, message.length);
    System.arraycopy(message, 0, tuple, tuple.length - message.length, message.length);
    if (ctx.write(tuple)) {
      ctx.write();
    } else {
      logger.error("Failed to deliver reset ack message. {} send buffers are full.", ctx);
      throw new RuntimeException("Failed to deliver reset ack message. " + ctx + "send buffers are full.");
    }
  }


  private volatile Subscriber subscriber;

  /**
   *
   * @param request
   * @param key
   */
  private void handleSubscriberRequest(final SubscribeRequestTuple request, final SelectionKey key)
  {
    logger.info("handling subscriber request...");
    try {
      serverHelperExecutor.submit(new Runnable()
      {
        @Override
        public void run()
        {
          final String upstream_identifier = request.getUpstreamIdentifier();

          /*
           * if there is already a datalist registered for the type in which this client is interested,
           * then get a iterator on the data items of that data list. If the datalist is not registered,
           * then create one and register it. Hopefully this one would be used by future upstream nodes.
           */
          DataList dl = publisherBuffers.get(upstream_identifier);
          if (dl == null) {
            dl = Tuple.FAST_VERSION.equals(request.getVersion()) ?
                new FastDataList(upstream_identifier, blockSize, numberOfCacheBlocks, BACK_PRESSURE_ENABLED) :
                new DataList(upstream_identifier, blockSize, numberOfCacheBlocks, BACK_PRESSURE_ENABLED);
            DataList odl = publisherBuffers.putIfAbsent(upstream_identifier, dl);
            if (odl != null) {
              dl = odl;
            }
          }

          final String identifier = request.getIdentifier();
          final String type = request.getStreamType();
          final long skipWindowId = (long)request.getBaseSeconds() << 32 | request.getWindowId();
          final LogicalNode ln = new LogicalNode(identifier, upstream_identifier, type, dl
              .newIterator(skipWindowId), skipWindowId, eventloop);

          int mask = request.getMask();
          if (mask != 0) {
            for (Integer bs : request.getPartitions()) {
              ln.addPartition(bs, mask);
            }
          }
          final LogicalNode oln = subscriberGroups.put(type, ln);
          if (oln != null) {
            oln.boot();
          }

          switchToNetty((java.nio.channels.SocketChannel)key.channel());

          subscriber = new Subscriber(ln, request.getBufferSize());
          eventloop.submit(new Runnable()
          {
            @Override
            public void run()
            {
              key.attach(subscriber);
              subscriber.registered(key);
              subscriber.connected();
            }
          });
        }
      });
    } catch (RejectedExecutionException e) {
      logger.error("Received subscriber request {} after server {} termination. Disconnecting {}.", request, this, key.channel(), e);
      if (key.isValid()) {
        try {
          key.channel().close();
        } catch (IOException ioe) {
          logger.error("Failed to close channel {}", key.channel(), ioe);
        }
      }
    }
  }


  /**
   * This class handle the message send from subscriber except the first register message.
   * @author bright
   *
   */
  private static class SubscriberHandler extends ChannelInboundHandlerAdapter
  {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
      logger.error("Unexpected message from subscriber: {}", msg);
    }
  }

  public static class ExceptionHandler extends ChannelDuplexHandler {

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.info("Got exception: {}", cause.getMessage());
    }

//    @Override
//    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
//        ctx.write(msg, promise.addListener(new ChannelFutureListener() {
//            @Override
//            public void operationComplete(ChannelFuture future) {
//                if (!future.isSuccess()) {
//                    // Handle write exception here...
//                }
//            }
//        }));
//    }

    // ... override more outbound methods to handle their exceptions as well
  }

  public ChannelPipeline nettyPipeline;

  private void configNettyChannel(NioSocketChannel nettyChannel)
  {
    StringBuilder sb = new StringBuilder();
    SocketChannelConfig conf = nettyChannel.config();
    int sendBufferSize = conf.getSendBufferSize();
    sb.append("sendBufferSize: ").append(sendBufferSize).append("; ");

    logger.info("Configure: {}", sb.toString());

    conf.setSendBufferSize(2048000);
    conf.setAutoRead(false);

    sb.setLength(0);
    sendBufferSize = conf.getSendBufferSize();
    sb.append("sendBufferSize: ").append(sendBufferSize).append("; ");

    logger.info("Configure: {}", sb.toString());
  }

  private void switchToNetty(java.nio.channels.SocketChannel javaChannel)
  {
    NioSocketChannel nettyChannel = new NioSocketChannel(javaChannel);
    EventLoopGroup group = new NioEventLoopGroup();
    try {
      Bootstrap b = new Bootstrap();
      b.group(group);

      //nettyPipeline = nettyChannel.pipeline().addFirst(new SubscriberHandler());
      //nettyPipeline = nettyChannel.pipeline().addFirst(new SubscriberHandler()).addLast(new ByteArrayEncoder());  //.addFirst(new LoggingHandler(LogLevel.INFO))
      nettyPipeline = nettyChannel.pipeline().addFirst(new SubscriberHandler()).addLast(new ByteArrayEncoder()).addLast(new ExceptionHandler());
      io.netty.channel.EventLoop eventLoop = group.next();
      configNettyChannel(nettyChannel);
      nettyChannel.unsafe().register(eventLoop, new DefaultChannelPromise(nettyChannel, eventLoop));


      //      if(!nettyChannel.isRegistered()) {
      //        logger.error("Channel not registered yet.");
      //      }
      //nettyChannel.closeFuture().sync();
      logger.info("switched to netty. javaChannel: {}", javaChannel);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      //group.shutdownGracefully();
    }
  }

  private void handleSubscriberTeardown(final SelectionKey key)
  {
    try {
      final Subscriber subscriber = (Subscriber)key.attachment();
      if (subscriber != null) {
        serverHelperExecutor.submit(new Runnable()
        {
          @Override
          public void run()
          {
            try {
              final LogicalNode ln = subscriber.ln;
              if (ln != null) {
                ln.removeChannel(subscriber);
                if (ln.getPhysicalNodeCount() == 0) {
                  DataList dl = publisherBuffers.get(ln.getUpstream());
                  if (dl != null) {
                    logger.info("Removing ln {} from dl {}", ln, dl);
                    dl.removeDataListener(ln);
                  }
                  subscriberGroups.remove(ln.getGroup(), ln);
                  ln.getIterator().close();
                }
                subscriber.ln = null;
              }
            } catch (Throwable t) {
              logger.error("Buffer server {} failed to tear down subscriber {}.", Server.this, subscriber, t);
            }
          }

          @Override
          public String toString()
          {
            return subscriber + " teardown task.";
          }
        });
      } else {
        logger.error("Selection key {} has unexpected attachment {}.", key, key.attachment());
      }
    } catch (ClassCastException e) {
      logger.error("Selection key {} has unexpected attachment {}.", key, key.attachment());
    } catch (RejectedExecutionException e) {
      logger.error("Subscriber {} teardown after server {} termination.", key.attachment(), this, e);
    }
  }

  /**
   *
   * @param request
   * @param connection
   * @return
   */
  public DataList handlePublisherRequest(PublishRequestTuple request, AbstractLengthPrependerClient connection)
  {
    String identifier = request.getIdentifier();
    logger.info("handling publisher request. identifier {}...", identifier);

    DataList dl = publisherBuffers.get(identifier);

    if (dl != null) {
      /*
       * close previous connection with the same identifier which is guaranteed to be unique.
       */
      AbstractLengthPrependerClient previous = publisherChannels.put(identifier, connection);
      if (previous != null) {
        eventloop.disconnect(previous);
      }

      try {
        dl.rewind(request.getBaseSeconds(), request.getWindowId());
      } catch (IOException ie) {
        throw new RuntimeException(ie);
      }
    } else {
      dl = Tuple.FAST_VERSION.equals(request.getVersion()) ?
          new FastDataList(identifier, blockSize, numberOfCacheBlocks, BACK_PRESSURE_ENABLED) :
          new DataList(identifier, blockSize, numberOfCacheBlocks, BACK_PRESSURE_ENABLED);
      DataList odl = publisherBuffers.putIfAbsent(identifier, dl);
      if (odl != null) {
        dl = odl;
      }
    }
    dl.setSecondaryStorage(storage, storageHelperExecutor);

    return dl;
  }

  @Override
  public ClientListener getClientConnection(SocketChannel sc, ServerSocketChannel ssc)
  {
    ClientListener client;
    if (authToken == null) {
      client = new UnidentifiedClient();
    } else {
      AuthClient authClient = new AuthClient();
      authClient.setToken(authToken);
      client = authClient;
    }
    return client;
  }

  @Override
  public void handleException(Exception cce, EventLoop el)
  {
    if (cce instanceof RuntimeException) {
      throw (RuntimeException)cce;
    }

    throw new RuntimeException(cce);
  }

  class AuthClient extends com.datatorrent.bufferserver.client.AuthClient
  {
    boolean ignore;

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      if (ignore) {
        return;
      }

      authenticateMessage(buffer, offset, size);

      unregistered(key);
      UnidentifiedClient client = new UnidentifiedClient();
      key.attach(client);
      key.interestOps(SelectionKey.OP_READ);
      client.registered(key);
      client.connected();

      int len = writeOffset - readOffset - size;
      if (len > 0) {
        client.transferBuffer(buffer, readOffset + size, len);
      }

      ignore = true;
    }
  }

  class UnidentifiedClient extends SeedDataClient
  {
    boolean ignore;

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      if (ignore) {
        return;
      }

      Tuple request = Tuple.getTuple(buffer, offset, size);
      switch (request.getType()) {
        case PUBLISHER_REQUEST:

          /*
           * unregister the unidentified client since its job is done!
           */
          unregistered(key);
          logger.info("Received publisher request: {}", request);
          PublishRequestTuple publisherRequest = (PublishRequestTuple)request;

          DataList dl = handlePublisherRequest(publisherRequest, this);
          dl.setAutoFlushExecutor(serverHelperExecutor);

          Publisher publisher;
          if (publisherRequest.getVersion().equals(Tuple.FAST_VERSION)) {
            publisher = new Publisher(dl, (long)request.getBaseSeconds() << 32 | request.getWindowId())
            {
              @Override
              public int readSize()
              {
                if (writeOffset - readOffset < 2) {
                  return -1;
                }

                short s = buffer[readOffset++];
                return s | (buffer[readOffset++] << 8);
              }

            };
          } else {
            publisher = new Publisher(dl, (long)request.getBaseSeconds() << 32 | request.getWindowId());
          }

          logger.info("The handler of server for publisher client switch to: {}; {}", publisher, System.identityHashCode(publisher));

          key.attach(publisher);
          key.interestOps(SelectionKey.OP_READ);
          publisher.registered(key);

          int len = writeOffset - readOffset - size;
          if (len > 0) {
            publisher.transferBuffer(this.buffer, readOffset + size, len);
          }
          ignore = true;

          break;

        case SUBSCRIBER_REQUEST:
          /*
           * unregister the unidentified client since its job is done!
           */
          unregistered(key.interestOps(0));
          ignore = true;
          logger.info("Received subscriber request: {}", request);

          handleSubscriberRequest((SubscribeRequestTuple)request, key);
          break;

        case PURGE_REQUEST:
          logger.info("Received purge request: {}", request);
          try {
            handlePurgeRequest((PurgeRequestTuple)request, this);
          } catch (IOException io) {
            throw new RuntimeException(io);
          }
          break;

        case RESET_REQUEST:
          logger.info("Received reset all request: {}", request);
          try {
            handleResetRequest((ResetRequestTuple)request, this);
          } catch (IOException io) {
            throw new RuntimeException(io);
          }
          break;

        default:
          throw new RuntimeException("unexpected message: " + request.toString());
      }
    }

  }

  private class Subscriber extends WriteOnlyLengthPrependerClient
  {
    private LogicalNode ln;

    Subscriber(LogicalNode ln, int bufferSize)
    {
      super(1024 * 1024, bufferSize == 0 ? 256 * 1024 : bufferSize);
      this.ln = ln;
      ln.addConnection(this);
    }

    @Override
    public void connected()
    {
      super.connected();
      serverHelperExecutor.submit(new Runnable()
      {
        @Override
        public void run()
        {
          final DataList dl = publisherBuffers.get(ln.getUpstream());
          if (dl != null) {
            ln.catchUp();
            dl.addDataListener(ln);
          } else {
            logger.error("Disconnecting {} with no matching data list.", this);
            ln.boot();
          }
        }
      });
    }

    @Override
    public void unregistered(final SelectionKey key)
    {
      handleSubscriberTeardown(key);
      super.unregistered(key);
    }

    @Override
    public String toString()
    {
      return getClass().getSimpleName() + '@' + Integer.toHexString(hashCode()) + "{ln=" + ln + "}";
    }

    //private List<ChannelFuture> writeFutures = Lists.newArrayList();
    //private List<Pair<ChannelFuture, Slice>> futureSlicePairs = Lists.newArrayList();
    private int writeCount = 0;

    /**
     * this method is called by handle the selection key.
     * The netty implementation should stand out of the netty eventloop.
     */
    @Override
    public void write() throws IOException
    {
      writeQueueDataPackaged();
    }

    private AtomicLong sentBlocks = new AtomicLong(0);
    private long requestSendBlocks = 0;


    /**
     * The input data should already prefixed with length
     * @param buffer
     * @param offset
     * @param length
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void sendData(byte[] buffer, int offset, int length)
    {
//      logger.info("sending: {}", new Slice(buffer, offset, length));
      ++requestSendBlocks;


      //send byte[] as object and Unpooled.wrappedBuffer are different.
      //Unpooled.wrappedBuffer seems add some head before the message
      //how to send byte[] using ByteBuf??
      //ChannelFuture writeFuture = nettyPipeline.writeAndFlush(Unpooled.wrappedBuffer(buffer, offset, length));

      //copy the data even if buffer is just fully occupied. As the buffer will be reused for write not data while sending
      byte[] sendBuffer = new byte[length];
      System.arraycopy(buffer, offset, sendBuffer, 0, length);

      //write(byte[]) must configure with ByteArrayEncoder
      //ChannelFuture writeFuture = nettyPipeline.writeAndFlush(sendBuffer);
      ChannelFuture writeFuture = nettyPipeline.write(sendBuffer);
      writeFuture.addListener(new GenericFutureListener(){
        @Override
        public void operationComplete(Future future) throws Exception
        {
          if(!future.isSuccess()) {
            logger.warn("A package send failed due to: " + future.cause().getMessage());
          }
          if(future.isCancellable()) {
            logger.warn("A package was cancelled due to: " + future.cause().getMessage());
          }
          if (future.isDone()) {
            sentBlocks.incrementAndGet();
          }
        }
      });

      //writeFutures.add(writeFuture);
      //futureSlicePairs.add(new Pair<>(writeFuture, slice));
      ++writeCount;
    }


    public int writeLength(byte[] buffer, int offset, int length)
    {
      int lengthOfLength = 0;
      while ((length & ~0x7F) != 0) {
        buffer[offset + lengthOfLength++] = (byte)((length & 0x7F) | 0x80);
        length >>>= 7;
      }
      buffer[offset + lengthOfLength++] = (byte)length;
      return lengthOfLength;
    }

    private void sleepSlient(long millis, int nanos)
    {
      try {
        //force to flush
        Thread.sleep(millis, nanos);
      } catch (InterruptedException e) {
        logger.warn("sleep exception.", e);
      }
    }

    //probably should not enable asynSend, as need another mechanism to remove the data from queue if create a task to send data.
    private final int DEFAULT_SEND_BUFFER_SIZE = 102400;
    private int bufferSize = DEFAULT_SEND_BUFFER_SIZE;
    private byte[] buffer = new byte[bufferSize];
    private int cacheFullCount = 0;
    private final int DEFAULT_NUM_OF_SEND_BUFFER = 100;
    private int maxCacheBlocks = DEFAULT_NUM_OF_SEND_BUFFER;

    public void writeQueueDataPackaged()
    {
      if(checkCacheFull()) {
        return;
      }

      int cachedLen = 0;
      boolean flushed = true;
      while (sendQueue.size() > 0) {
        /**
         * The slices will be reused. But in netty case, we can't make sure the data has been sent.
         * So, even clean the buffer after sent can't solve the "Unexpected slice" issue. see WriteOnlyClient.send
         * So, clone the slice to solve the problem.
         */
        final Slice oldSlice = sendQueue.poll();
        final Slice slice = new Slice(oldSlice.buffer, oldSlice.offset, oldSlice.length);
        oldSlice.buffer = null;
        freeQueue.offer(oldSlice);

        if(cachedLen + slice.length + 4 >= bufferSize) {
          if(cachedLen > 0) {
            //send cached first
            sendData(buffer, 0, cachedLen);
            flushed = false;
            cachedLen = 0;
          }
        }

        if(slice.length + 4 >= bufferSize) {
          //the cached data must have been sent.
          buffer = new byte[slice.length + 4];
        }
        //concatenate this slice
        int lenOfLen = writeLength(buffer, cachedLen, slice.length);
        System.arraycopy(slice.buffer, slice.offset, buffer, cachedLen + lenOfLen, slice.length);
        cachedLen += slice.length + lenOfLen;



        //force to flush
        if (!flushed) {
          nettyPipeline.flush();
          flushed = true;
        }

        if(checkCacheFull()) {
          return;
        }

      }
      if(cachedLen > 0) {
        sendData(buffer, 0, cachedLen);
        nettyPipeline.flush();
      }
    }

    private boolean checkCacheFull()
    {
      long sentBlocks1 = sentBlocks.get();
      if(requestSendBlocks >= sentBlocks1 + maxCacheBlocks) {
        if(cacheFullCount++ % 2000 == 0) {
          logger.info("cache full. requestSendBlocks: {}, sentBlocks: {}; cached blocks: {}; this: {}", requestSendBlocks, sentBlocks1, requestSendBlocks - sentBlocks1, System.identityHashCode(this));
        }

        //50 seems the best number for tuple of 100
        sleepSlient(0, 50);
        return true;
      }

      return false;
    }
  }

  /**
   * When the publisher connects to the server and starts publishing the data,
   * this is the end on the server side which handles all the communication.
   *
   */
  class Publisher extends SeedDataClient
  {
    private final DataList datalist;
    boolean dirty;

    Publisher(DataList dl, long windowId)
    {
      super(dl.getBuffer(windowId), dl.getPosition(), 1024);
      this.datalist = dl;
    }

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      //if (buffer[offset] == MessageType.BEGIN_WINDOW_VALUE || buffer[offset] == MessageType.END_WINDOW_VALUE) {
      //  logger.debug("server received {}", Tuple.getTuple(buffer, offset, size));
      //}
      //logger.info("Publisher received a message: {}", new Slice(buffer, offset, size));
      dirty = true;
    }

    /**
     * Schedules a task to conditionally resume I/O channel read operations.
     * No-op if {@linkplain java.nio.channels.SelectionKey#OP_READ OP_READ}
     * is already set in the key {@linkplain java.nio.channels.SelectionKey#interestOps() interestOps}.
     * Otherwise, calls {@linkplain #read(int) read(0)} to process data
     * left in the Publisher read buffer and registers {@linkplain java.nio.channels.SelectionKey#OP_READ OP_READ}
     * in the key {@linkplain java.nio.channels.SelectionKey#interestOps() interestOps}.
     * @return true
     */
    @Override
    public boolean resumeReadIfSuspended()
    {
      eventloop.submit(new Runnable()
      {
        @Override
        public void run()
        {
          final int interestOps = key.interestOps();
          if ((interestOps & SelectionKey.OP_READ) == 0) {
            if (readExt(0)) {
              logger.debug("Resuming read on key {} with attachment {}", key, key.attachment());
              key.interestOps(interestOps | SelectionKey.OP_READ);
            } else {
              logger.debug("Keeping read on key {} with attachment {} suspended. ", key, key.attachment(), datalist);
              datalist.notifyListeners();
            }
          }
        }
      });
      return true;
    }

    @Override
    public void read(int len)
    {
      readExt(len);

      //trigger send of the subscriber. use eventloop to send.
//      if (subscriber != null) {
//        eventloop.submit(new Runnable()
//        {
//          @Override
//          public void run()
//          {
//            subscriber.writeSendQueueData();
//          }
//        });
//      }

    }

    private boolean readExt(int len)
    {
      //logger.info("read {} bytes", len);
      writeOffset += len;
      do {
        if (size <= 0) {
          switch (size = readSize()) {
            case -1:
              if (writeOffset == buffer.length) {
                if (readOffset > writeOffset - 5) {
                  dirty = false;
                  datalist.flush(writeOffset);
                  /*
                   * if the data is not corrupt, we are limited by space to receive full varint.
                   * so we allocate a new byteBuffer and copy over the partially written data to the
                   * new byteBuffer and start as if we always had full room but not enough data.
                   */
                  if (!switchToNewBufferOrSuspendRead(buffer, readOffset, size  + VarInt.getSize(size))) {
                    return false;
                  }
                }
              } else if (dirty) {
                dirty = false;
                datalist.flush(writeOffset);
              }
              return true;

            case 0:
              continue;

            default:
              break;
          }
        }

        if (writeOffset - readOffset >= size) {
          onMessage(buffer, readOffset, size);
          readOffset += size;
          size = 0;
        } else {
          if (writeOffset == buffer.length) {
            dirty = false;
            datalist.flush(writeOffset);
            /*
             * hit wall while writing serialized data, so have to allocate a new byteBuffer.
             */
            if (!switchToNewBufferOrSuspendRead(buffer, readOffset - VarInt.getSize(size), size + VarInt.getSize(size))) {
              readOffset -= VarInt.getSize(size);
              size = 0;
              return false;
            }
            size = 0;
          } else if (dirty) {
            dirty = false;
            datalist.flush(writeOffset);
          }
          return true;
        }
      } while (true);
    }

    private boolean switchToNewBufferOrSuspendRead(final byte[] array, final int offset, final int size)
    {
      if (switchToNewBuffer(array, offset, size)) {
        return true;
      }
      datalist.suspendRead(this);
      return false;
    }

    private boolean switchToNewBuffer(final byte[] array, final int offset, final int size)
    {
      if (datalist.isMemoryBlockAvailable()) {
        final byte[] newBuffer = datalist.newBuffer(size);
        byteBuffer = ByteBuffer.wrap(newBuffer);
        if (array == null || array.length - offset == 0) {
          writeOffset = 0;
        } else {
          writeOffset = array.length - offset;
          System.arraycopy(buffer, offset, newBuffer, 0, writeOffset);
          byteBuffer.position(writeOffset);
        }
        buffer = newBuffer;
        readOffset = 0;
        datalist.addBuffer(buffer);
        return true;
      }
      return false;
    }

    @Override
    public void unregistered(final SelectionKey key)
    {
      super.unregistered(key);
      teardown();
    }

    @Override
    public void handleException(Exception cce, EventLoop el)
    {
      teardown();

      if (cce instanceof RejectedExecutionException && serverHelperExecutor.isTerminated()) {
        logger.warn("Terminated Executor Exception for {}.", this, cce);
        el.disconnect(this);
      } else {
        super.handleException(cce, el);
      }
    }

    @Override
    public String toString()
    {
      return getClass().getName() + '@' + Integer.toHexString(hashCode()) + " {datalist=" + datalist + '}';
    }

    private volatile boolean torndown;

    private void teardown()
    {
      //logger.debug("Teardown is being called {}", torndown, new Exception());
      if (torndown) {
        return;
      }
      torndown = true;

      /*
       * if the publisher unregistered, all the downstream guys are going to be unregistered anyways
       * in our world. So it makes sense to kick them out proactively. Otherwise these clients since
       * are not being written to, just stick around till the next publisher shows up and eat into
       * the data it's publishing for the new subscribers.
       */

      /**
       * since the publisher server died, the queue which it was using would stop pumping the data unless
       * a new publisher comes up with the same name. We leave it to the stream to decide when to bring up a new node
       * with the same identifier as the one which just died.
       */
      if (publisherChannels.containsValue(this)) {
        final Iterator<Entry<String, AbstractLengthPrependerClient>> i = publisherChannels.entrySet().iterator();
        while (i.hasNext()) {
          if (i.next().getValue() == this) {
            i.remove();
            break;
          }
        }
      }

      ArrayList<LogicalNode> list = new ArrayList<LogicalNode>();
      String publisherIdentifier = datalist.getIdentifier();
      Iterator<LogicalNode> iterator = subscriberGroups.values().iterator();
      while (iterator.hasNext()) {
        LogicalNode ln = iterator.next();
        if (publisherIdentifier.equals(ln.getUpstream())) {
          list.add(ln);
        }
      }

      for (LogicalNode ln : list) {
        ln.boot();
      }
    }

  }

  abstract class SeedDataClient extends AbstractLengthPrependerClient
  {

    public SeedDataClient()
    {
    }

    public SeedDataClient(int readBufferSize, int sendBufferSize)
    {
      super(readBufferSize, sendBufferSize);
    }

    public SeedDataClient(byte[] readbuffer, int position, int sendBufferSize)
    {
      super(readbuffer, position, sendBufferSize);
    }

    public void transferBuffer(byte[] array, int offset, int len)
    {
      int remainingCapacity;
      do {
        remainingCapacity = buffer.length - writeOffset;
        if (len < remainingCapacity) {
          remainingCapacity = len;
          byteBuffer.position(writeOffset + remainingCapacity);
        } else {
          byteBuffer.position(buffer.length);
        }
        System.arraycopy(array, offset, buffer, writeOffset, remainingCapacity);
        read(remainingCapacity);

        offset += remainingCapacity;
      } while ((len -= remainingCapacity) > 0);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(Server.class);
}
