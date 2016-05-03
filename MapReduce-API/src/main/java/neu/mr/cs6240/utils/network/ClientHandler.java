package neu.mr.cs6240.utils.network;

import static neu.mr.cs6240.Constants.NetworkCC.JOBSUBMITTER_STATUS_MSG;

import org.apache.log4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import neu.mr.cs6240.shared.JobStateMachine;
import neu.mr.cs6240.sharedobjects.JobState;

/**
 * Used to communicate with MRAppMaster
 * 
 * @author ajay subramanya
 *
 */
public class ClientHandler extends SimpleChannelInboundHandler<Object> {

	private final static Logger logger = Logger.getLogger(ClientHandler.class);

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("writting to MRAppMaster " + Client.job);
		ctx.writeAndFlush(Client.job);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object obj) throws Exception {
		JobStateMachine state = JobStateMachine.getInstance();
		state.setJobState((JobState) obj);
		if (obj == JobState.ERROR) {
			logger.error("[JOB FAILED] Master returned a failure message");
			ctx.close();
		}
		if (obj == JobState.FINISHED) {
			logger.info("[JOB FINISHED] please look into the s3 output path for the results");
			ctx.close();
		}
		logger.info("Job is in state : " + JobState.fromValue((JobState) obj));
	}

	/**
	 * check with MRAppMaster the status of the submitted job if it does not
	 * tell us anything in a window of 60 seconds
	 */
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleStateEvent e = (IdleStateEvent) evt;
			if (e.state() == IdleState.ALL_IDLE) {
				ctx.writeAndFlush(JOBSUBMITTER_STATUS_MSG);
			}
		}
	}

}
