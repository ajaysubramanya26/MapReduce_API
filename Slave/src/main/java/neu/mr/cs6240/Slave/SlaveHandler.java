package neu.mr.cs6240.Slave;

import static neu.mr.cs6240.Constants.NetworkCC.SLAVE_HEARTBEAT_MSG;
import static neu.mr.cs6240.Constants.NetworkCC.SLAVE_READY_MSG;

import org.apache.log4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * This is the first handler in the pipeline, does not handle any incoming
 * message, on start just writes the slave ready message to the master and then
 * sends heart-beat every 60 seconds
 * 
 * 
 * @author smitha
 * @author ajay
 *
 */
public class SlaveHandler extends SimpleChannelInboundHandler<Object> {

	/**
	 * always return false as this class is only for writing Slave status to
	 * master
	 */
	@Override
	public boolean acceptInboundMessage(Object msg) throws Exception {
		return false;
	}

	final Logger logger = Logger.getLogger(SlaveHandler.class);

	/**
	 * @param ctx
	 *            the context to which you write and if there are no more
	 *            handlers in the pipeline then what ever you write to context
	 *            will be written to the channel
	 */
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("Slave " + ctx.channel().localAddress() + " writing to server");
		ctx.writeAndFlush(SLAVE_READY_MSG);
	}

	/**
	 * Send Heart Beat signal to Master when channel is idle
	 */
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleStateEvent e = (IdleStateEvent) evt;
			if (e.state() == IdleState.ALL_IDLE) {
				ctx.writeAndFlush(SLAVE_HEARTBEAT_MSG);
			}
		}
	}

	/**
	 * does nothing , the task object would be passed on to the next Handler in
	 * the pipeline
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {}
}
