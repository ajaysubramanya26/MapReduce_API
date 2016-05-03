package neu.mr.cs6240.Slave;

import org.apache.log4j.Logger;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * Creates pipelines to handle different type of objects from Master.
 *
 * @author smitha
 *
 */
public class SlaveInitializer extends ChannelInitializer<SocketChannel> {

	final Logger logger = Logger.getLogger(SlaveInitializer.class);

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {

		ChannelPipeline pipeline = ch.pipeline();

		pipeline.addLast(new ObjectEncoder());
		pipeline.addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)));

		// send heart beat to server when channel is idle every 60 seconds
		pipeline.addLast("idleStateHandler", new IdleStateHandler(60, 60, 60));

		// business logic.
		pipeline.addLast(new SlaveHandler());
		pipeline.addLast(new MapHandler());
		pipeline.addLast(new ReduceHandler());
		pipeline.addLast(new LogHandler());

	}
}
