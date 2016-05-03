package neu.mr.cs6240.JobReceiver;

import org.apache.log4j.Logger;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

/**
 * JobInitializer class to add channel pipeline
 *
 * @author smitha
 *
 */
public class JobInitializer extends ChannelInitializer<SocketChannel> {

	final Logger logger = Logger.getLogger(JobInitializer.class);

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ChannelPipeline pipeline = ch.pipeline();

		// pipeline.addLast(new LoggingHandler(LogLevel.INFO));
		pipeline.addLast(new ObjectEncoder());
		pipeline.addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)));

		// business logic.
		pipeline.addLast(new JobHandler());

	}

}
