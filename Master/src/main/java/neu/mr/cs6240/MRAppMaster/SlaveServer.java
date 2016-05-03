package neu.mr.cs6240.MRAppMaster;

import static neu.mr.cs6240.Constants.NetworkCC.SLAVE_CONN_PORT;

import org.apache.log4j.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import neu.mr.cs6240.StateMachine.SlaveServerStateMachine;

/**
 * Starts the Slave server socket. So slaves can communicate with the master
 * node using this socket channel.
 *
 * @author smitha
 *
 */
public class SlaveServer {

	public static int numOfSlaves = 1; // Default

	public static void startTaskServer(String[] args) throws InterruptedException {

		final Logger logger = Logger.getLogger(SlaveServer.class);

		if (args.length != 1) {
			logger.error("Number of nodes not specified");
			System.exit(-1);
		}

		try {
			numOfSlaves = Integer.parseInt(args[0]);
		} catch (NumberFormatException e) {
			logger.error("Invalid Number", e);
			System.exit(-1);
		}

		SlaveServerStateMachine.getInstance().setNumOfSlavesExpected(numOfSlaves);

		EventLoopGroup master = new NioEventLoopGroup(1);
		EventLoopGroup worker = new NioEventLoopGroup();
		try {
			logger.info("bootstraping slave server");
			ServerBootstrap b = new ServerBootstrap().group(master, worker).channel(NioServerSocketChannel.class)
					.childHandler(new SlaveServerInitializer()).childOption(ChannelOption.SO_KEEPALIVE, true)
					.option(ChannelOption.SO_KEEPALIVE, true);

			// Start the server.
			ChannelFuture f = b.bind(SLAVE_CONN_PORT).sync();
			// used for closing
			SlaveServerStateMachine.getInstance().setMasterSlaveServerChannel(f.channel());

			// Wait until the server socket is closed.
			f.channel().closeFuture().sync();
		} finally {
			logger.info("shutting down master slave server");
			worker.shutdownGracefully();
			master.shutdownGracefully();
		}
		// inform the main(MRAppMaster) thread that its done
		throw new InterruptedException();
	}

}
