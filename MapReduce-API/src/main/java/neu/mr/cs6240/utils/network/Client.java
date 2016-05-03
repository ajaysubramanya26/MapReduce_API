package neu.mr.cs6240.utils.network;

import static neu.mr.cs6240.Constants.NetworkCC.MASTER_IP_FILE;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import neu.mr.cs6240.mapred.Job;

/**
 * Class to send and receive data from MRAppMaster
 *
 * @author ajay subramanya
 *
 */
public class Client {
	static final int MASTER_PORT = Integer.parseInt(System.getProperty("port", "8991"));
	private final static Logger logger = Logger.getLogger(Client.class);
	private String MASTER_IP;
	static final int CHANNEL_BUF_SIZE = 128000000;
	public static Job job;

	public Client(Job job) {
		Client.job = job;
		getMasterIp();
	}

	/**
	 * Bootstrap's the client and sends the job to MRAppMaster
	 *
	 * @author ajay subramanya
	 */
	public void sendJob() {
		logger.info("sending job to MRAppMaster");
		EventLoopGroup worker = new NioEventLoopGroup();
		try {
			Bootstrap b = new Bootstrap().group(worker).channel(NioSocketChannel.class)
					.option(ChannelOption.SO_KEEPALIVE, true).handler(new ClientInitializer())
					.option(ChannelOption.SO_SNDBUF, CHANNEL_BUF_SIZE)
					.option(ChannelOption.SO_RCVBUF, CHANNEL_BUF_SIZE);
			try {
				// Start the client
				ChannelFuture f = b.connect(MASTER_IP, MASTER_PORT).sync();

				// Wait until the connection is closed.
				f.channel().closeFuture().sync();

				logger.info("Channel with MRAppMaster closed");
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
			}
		} finally {
			worker.shutdownGracefully();
		}
	}

	/**
	 * to read master IP that provision writes
	 */
	private void getMasterIp() {
		this.MASTER_IP = "127.0.0.1";
		try {
			this.MASTER_IP = FileUtils.readFileToString(new File(MASTER_IP_FILE));
		} catch (IOException e) {
			logger.warn("exception reading master IP ");
		}
	}
}
