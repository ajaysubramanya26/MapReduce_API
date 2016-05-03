package neu.mr.cs6240.MRAppMaster;

import static neu.mr.cs6240.Constants.NetworkCC.JOB_READY_MSG;
import static neu.mr.cs6240.Constants.NetworkCC.SLAVE_HEARTBEAT_MSG;
import static neu.mr.cs6240.Constants.NetworkCC.SLAVE_READY_MSG;

import org.apache.log4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import neu.mr.cs6240.StateMachine.JobStateObserver;
import neu.mr.cs6240.StateMachine.SlaveServerStateMachine;
import neu.mr.cs6240.TaskScheduler.MapTaskHandler;
import neu.mr.cs6240.TaskScheduler.TaskResultHandler;
import neu.mr.cs6240.sharedobjects.JobState;
import neu.mr.cs6240.sharedobjects.TaskResult;

/**
 * Slave Server Handler to receive TaskResult messages
 *
 * @author smitha
 *
 */
public class SlaveServerHandler extends SimpleChannelInboundHandler<Object> {

	final Logger logger = Logger.getLogger(SlaveServerHandler.class);

	final SlaveServerStateMachine slaveSM = SlaveServerStateMachine.getInstance();

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("Slave Server: Channel active");
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {

		if (msg instanceof String) {
			handleStringMsgs(ctx, msg);
		} else if (msg instanceof TaskResult) {
			TaskResult taskres = (TaskResult) msg;
			logger.info("Received " + taskres.toString());
			TaskResultHandler.getInstance().onReceiveTaskResult(ctx.channel(), taskres);
		}
	}

	/**
	 * Handles all string messages from clients
	 *
	 * @param ctx
	 * @param msg
	 */
	private void handleStringMsgs(ChannelHandlerContext ctx, Object msg) {

		// begin processing job
		if (msg.equals(JOB_READY_MSG)) {
			logger.info("Received " + msg);
			JobStateObserver observer = JobStateObserver.getInstance();
			logger.info(observer.getJobState());
			slaveSM.printSlaveIptoIds();
			slaveSM.updateJobStatusInformSubmitter(JobState.SUBMITTED);
			boolean result = MapTaskHandler.getInstance().onReceiveJobMsg(observer.getJob());
			if (result == false) {
				logger.error("Job Submission Failed");
				slaveSM.onError();
			}
		}

		// add to state machine and wait for job
		if (msg.equals(SLAVE_READY_MSG)) {
			logger.info("Received " + msg);
			slaveSM.addSlaveChannel(ctx.channel());
			slaveSM.printSlaveIptoIds();
		}

		// slave heart beat
		if (msg.equals(SLAVE_HEARTBEAT_MSG)) {
			if (logger.isDebugEnabled()) {
				logger.debug("Received " + msg + " " + ctx.channel().remoteAddress().toString());
			}
			slaveSM.updateHeartBeat(ctx.channel());
		}
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		slaveSM.onError(); // this closes the context
		// ctx.close();
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		// remove the Slave from SlavesConnected
		slaveSM.removeSlaveChannel(ctx.channel());
		ctx.fireChannelUnregistered();
	}
}
