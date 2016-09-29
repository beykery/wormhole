/*
 * Copyright 2016 beykery.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.beykery.wormhole;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 *
 * @author beykery
 */
public class WormRegistry
{

  private final NioDatagramChannel channel;
  private final NioEventLoopGroup workers;
  private final Map<String, Set<Endpoint>> services;

  public WormRegistry(int port, int workerSize)
  {
    if (port <= 0 || workerSize <= 0)
    {
      throw new IllegalArgumentException("参数非法");
    }
    services = new HashMap<>();
    workers = new NioEventLoopGroup(workerSize);
    final NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup();
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.channel(NioDatagramChannel.class);
    bootstrap.group(nioEventLoopGroup);
    bootstrap.handler(new ChannelInitializer<NioDatagramChannel>()
    {

      @Override
      protected void initChannel(NioDatagramChannel ch) throws Exception
      {
        ChannelPipeline cp = ch.pipeline();
        cp.addLast(new ChannelInboundHandlerAdapter()
        {
          @Override
          public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
          {
            DatagramPacket dp = (DatagramPacket) msg;
            WormRegistry.this.onMessage(dp);
          }

          @Override
          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
          {
            WormRegistry.this.onException(cause);
          }
        });
      }
    });
    ChannelFuture sync = bootstrap.bind(port).syncUninterruptibly();
    channel = (NioDatagramChannel) sync.channel();
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        nioEventLoopGroup.shutdownGracefully();
      }
    }));
  }

  /**
   * message
   *
   * @param dp
   */
  private void onMessage(final DatagramPacket dp)
  {
    this.workers.execute(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          ByteBuf bb = dp.content();
          int cmd = bb.readByte();
          switch (cmd)
          {
            case 0://regist
              String name = readUtf(bb);
              String inner = readUtf(bb);
              InetSocketAddress sender = dp.sender();
              WormRegistry.this.addEndpoint(new Endpoint(name, inner, sender));
              break;
            case 1://check
               name = readUtf(bb);
               
          }
        } catch (Exception ex)
        {
          //do nothing
        }
      }

      private String readUtf(ByteBuf bb) throws UnsupportedEncodingException
      {
        int len = bb.readInt();
        byte[] c = new byte[len];
        bb.readBytes(c);
        return new String(c, "utf-8");
      }
    });
  }

  /**
   * exception
   *
   * @param cause
   */
  private void onException(Throwable cause)
  {
    //do nothing
  }

  private synchronized void addEndpoint(Endpoint end)
  {
    Set<Endpoint> set = this.services.get(end.name);
    if (set == null)
    {
      set = new HashSet<>();
      this.services.put(end.name, set);
    }
    set.add(end);
  }

  class Endpoint
  {

    String name;
    String inner;
    InetSocketAddress addr;

    public Endpoint(String name, String inner, InetSocketAddress addr)
    {
      this.name = name;
      this.inner = inner;
      this.addr = addr;
    }

    @Override
    public int hashCode()
    {
      int hash = 5;
      hash = 29 * hash + Objects.hashCode(this.name);
      hash = 29 * hash + Objects.hashCode(this.inner);
      hash = 29 * hash + Objects.hashCode(this.addr);
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj)
      {
        return true;
      }
      if (obj == null)
      {
        return false;
      }
      if (getClass() != obj.getClass())
      {
        return false;
      }
      final Endpoint other = (Endpoint) obj;
      if (!Objects.equals(this.name, other.name))
      {
        return false;
      }
      if (!Objects.equals(this.inner, other.inner))
      {
        return false;
      }
      return Objects.equals(this.addr, other.addr);
    }

  }

}
