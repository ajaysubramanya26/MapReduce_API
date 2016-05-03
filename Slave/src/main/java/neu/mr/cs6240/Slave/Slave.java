package neu.mr.cs6240.Slave;

import static neu.mr.cs6240.Constants.NetworkCC.CHANNEL_BUF_SIZE;
import static neu.mr.cs6240.Constants.NetworkCC.SLAVE_CONN_PORT;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import neu.mr.cs6240.StateMachine.SlaveStateMachine;

/**
 * Slave main class. Waits for Tasks form master.
 *
 * @author smitha
 *
 */
public class Slave {

	public static void main(String[] args) {
		String log4jConfPath = "./log4jS.properties";
		PropertyConfigurator.configure(log4jConfPath);
		final Logger logger = Logger.getLogger(Slave.class);

		if (args.length != 1) {
			logger.error("Master's IP not specified Format: <IP>");
			System.exit(-1);
		}

		SlaveStateMachine.getInstance().setMasterIp(args[0]);

		logger.info("Connecting to master with ip " + args[0] + " on port " + SLAVE_CONN_PORT);
		EventLoopGroup worker = new NioEventLoopGroup();
		try {
			logger.info("Bootstraping slave client");
			Bootstrap b = new Bootstrap().group(worker).channel(NioSocketChannel.class)
					.option(ChannelOption.SO_KEEPALIVE, true).handler(new SlaveInitializer())
					.option(ChannelOption.SO_SNDBUF, CHANNEL_BUF_SIZE)
					.option(ChannelOption.SO_RCVBUF, CHANNEL_BUF_SIZE);
			try {

				// Start the client
				ChannelFuture f = b.connect(args[0], SLAVE_CONN_PORT).sync();

				// Wait until the connection is closed.
				f.channel().closeFuture().sync();
				logger.info("Channel closed");
			} catch (InterruptedException e) {
				logger.error("Slave Interrupted", e);
			}
		} finally {
			worker.shutdownGracefully();
		}
	}

}
