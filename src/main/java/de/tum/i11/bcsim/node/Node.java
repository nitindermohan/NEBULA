package de.tum.i11.bcsim.node;

import com.google.protobuf.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.logging.Logger;

public class Node {
    private static final Logger LOGGER = Logger.getLogger(Node.class.getName());

    private InetSocketAddress listenAddr;
    private final EventLoopGroup bossGroup, workerGroup;
    private final ServerBootstrap sb;
    private final Bootstrap cb;

    public Node(ChannelHandler handler, Supplier<Message> msgSupplier) {
        LOGGER.finest("Starting node event loops");
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        sb = new ServerBootstrap();
        NodeInitializer initializer = new NodeInitializer(handler, msgSupplier);
        sb.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(initializer)
                .option(ChannelOption.SO_BACKLOG, 512)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        cb = new Bootstrap();
        cb.group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(initializer)
                .option(ChannelOption.SO_KEEPALIVE, true);
    }

    /**
     * Bind this node to listen for TCP connections on the given address
     * @param listenAddr the listening address
     * @return A future completed once this node was bound to the listening address
     */
    public ChannelFuture bind(InetSocketAddress listenAddr) {
        LOGGER.finer("Binding to "+listenAddr);
        ChannelFuture f = sb.bind(listenAddr).syncUninterruptibly();
        LOGGER.finer("Listening on local: "+f.channel().localAddress()+", remote: "+f.channel().remoteAddress());
        this.listenAddr = (InetSocketAddress) f.channel().localAddress();
        return f;
    }

    /**
     * Bind this node to listen for TCP connections on the given address
     * @param host the listening address
     * @return A future completed once this node was bound to the listening address
     */
    public ChannelFuture bind(InetAddress host) {
        return bind(new InetSocketAddress(host, 0));
    }

    public InetSocketAddress getListenAddr() {
        return listenAddr;
    }

    /**
     * Establish a TCP connection to the given Address
     * @param addr the address to connect to
     * @return A future to the established channel
     */
    public ChannelFuture connect(InetSocketAddress addr) {
        LOGGER.finer("Connecting to "+addr);
        return cb.connect(addr);
    }

    /**
     * Close this node gracefully
     * @return A future completed once this node is closed
     */
    public CompletableFuture<Void> close() {

            LOGGER.finer("Closing Node");

            CompletableFuture<Void> f1 = new CompletableFuture<>();
            CompletableFuture<Void> f2 = new CompletableFuture<>();

            bossGroup.shutdownGracefully().addListener(f -> f1.complete(null));
            workerGroup.shutdownGracefully().addListener(f -> f2.complete(null));

            LOGGER.finer("Closed Node");

            return CompletableFuture.allOf(f1,f2);
    }

    /**
     * @return true iff this node was closed
     */
    public boolean isClosed() {
        return bossGroup.isShutdown() && bossGroup.isTerminated() && workerGroup.isShutdown() && workerGroup.isTerminated();
    }
}
