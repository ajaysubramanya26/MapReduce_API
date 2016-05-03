package neu.mr.cs6240.JobReceiver;

import static neu.mr.cs6240.Constants.NetworkCC.CHANNEL_BUF_SIZE;
import static neu.mr.cs6240.Constants.NetworkCC.JOB_RECEIVE_PORT;

import org.apache.log4j.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import neu.mr.cs6240.StateMachine.SlaveServerStateMachine;

/**
 * Job Server listening to job submit requests.
 *
 * @author smitha
 *
 */
public class JobServer {

	public static void startJobServer(String[] args) throws InterruptedException {

		final Logger logger = Logger.getLogger(JobServer.class);

		EventLoopGroup master = new NioEventLoopGroup(1);
		EventLoopGroup worker = new NioEventLoopGroup();
		try {
			logger.info("bootstraping job receive server");
			ServerBootstrap b = new ServerBootstrap().group(master, worker).channel(NioServerSocketChannel.class)
					.childHandler(new JobInitializer()).childOption(ChannelOption.SO_KEEPALIVE, true)
					.option(ChannelOption.SO_KEEPALIVE, true).option(ChannelOption.SO_RCVBUF, CHANNEL_BUF_SIZE)
					.option(ChannelOption.SO_SNDBUF, CHANNEL_BUF_SIZE);

			// Start the server.
			ChannelFuture f = b.bind(JOB_RECEIVE_PORT).sync();

			SlaveServerStateMachine.getInstance().setMasterJobServerChannel(f.channel());

			// Wait until the server socket is closed.
			f.channel().closeFuture().sync();

		} finally {
			logger.info("shutting down master job server");
			worker.shutdownGracefully();
			master.shutdownGracefully();
		}
		// inform the main(MRAppMaster) thread that its done
		throw new InterruptedException();
	}
}
