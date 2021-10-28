package de.tum.i11.bcsim.node;

import com.google.protobuf.Message;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import java.util.function.Supplier;

public class NodeInitializer extends ChannelInitializer<SocketChannel> {

    private ChannelHandler handler;
    private Supplier<Message> messageSupplier;

    NodeInitializer(ChannelHandler handler, Supplier<Message> messageSupplier) {
        this.handler = handler;
        this.messageSupplier = messageSupplier;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        // Add Protobuf boilerplate
        ChannelPipeline p = ch.pipeline();
        p.addLast(new ProtobufVarint32FrameDecoder());
        p.addLast(new ProtobufDecoder(messageSupplier.get()));

        p.addLast(new ProtobufVarint32LengthFieldPrepender());
        p.addLast(new ProtobufEncoder());

        p.addLast(handler);
    }
}
