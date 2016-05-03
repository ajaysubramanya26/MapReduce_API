package neu.mr.cs6240.utils.network;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * creates a pipeline and adds handlers to it
 *
 * @author ajay subramanya
 *
 */
public class ClientInitializer extends ChannelInitializer<SocketChannel> {

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ChannelPipeline pipeline = ch.pipeline();

		pipeline.addLast(new ObjectEncoder());
		pipeline.addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)));

		// send heart beat to server when channel is idle every 60 seconds
		pipeline.addLast(new IdleStateHandler(60, 60, 60));

		// business logic.
		pipeline.addLast(new ClientHandler());
	}

}
