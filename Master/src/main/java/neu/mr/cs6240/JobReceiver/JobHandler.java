package neu.mr.cs6240.JobReceiver;

import static neu.mr.cs6240.Constants.NetworkCC.JOBSUBMITTER_STATUS_MSG;
import static neu.mr.cs6240.Constants.NetworkCC.JOB_READY_MSG;
import static neu.mr.cs6240.Constants.NetworkCC.SLAVE_CONN_PORT;

import java.net.InetAddress;

import org.apache.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import neu.mr.cs6240.MRAppMaster.SlaveServerInitializer;
import neu.mr.cs6240.StateMachine.JobStateObserver;
import neu.mr.cs6240.StateMachine.SlaveServerStateMachine;
import neu.mr.cs6240.mapred.Job;
import neu.mr.cs6240.sharedobjects.JobState;

/**
 * This class handles the Job received from Job Submitter.
 *
 * @author smitha
 *
 */
public class JobHandler extends SimpleChannelInboundHandler<Object> {

	final Logger logger = Logger.getLogger(JobHandler.class);

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("Job Server: Channel active");
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {

		if (msg instanceof String) {
			logger.info("Received " + msg);
			if (msg.equals(JOBSUBMITTER_STATUS_MSG)) {
				ctx.writeAndFlush(JobStateObserver.getInstance().getJobState());
			}
		}
		if (msg instanceof Job) {
			Job job = (Job) msg;
			JobStateObserver observer = JobStateObserver.getInstance();

			// set the job submitter channel
			SlaveServerStateMachine slaveSM = SlaveServerStateMachine.getInstance();
			slaveSM.addJobSubmitterChannel(ctx.channel());

			logger.info("Prior State " + observer.toString());
			if (job != null) {
				logger.info("job received" + job);
				observer.setJob(job);
				observer.setJobState(JobState.RECEIVED);
				EventLoopGroup worker = new NioEventLoopGroup();
				Bootstrap b = new Bootstrap().group(worker).channel(NioSocketChannel.class)
						.option(ChannelOption.SO_KEEPALIVE, true).handler(new SlaveServerInitializer());
				Channel ch = b.connect(InetAddress.getLocalHost().getHostName(), SLAVE_CONN_PORT).sync().channel();
				ch.writeAndFlush(JOB_READY_MSG);
			}
		}
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		ctx.close();
	}

}
