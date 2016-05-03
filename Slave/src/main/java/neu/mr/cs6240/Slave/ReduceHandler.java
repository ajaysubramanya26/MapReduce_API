package neu.mr.cs6240.Slave;

import org.apache.log4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import neu.mr.cs6240.StateMachine.SlaveStateMachine;
import neu.mr.cs6240.sharedobjects.ERR_CODE;
import neu.mr.cs6240.sharedobjects.Task;
import neu.mr.cs6240.sharedobjects.TaskResult;
import neu.mr.cs6240.sharedobjects.TaskType;
import neu.mr.cs6240.utils.io.S3IO;
import neu.mr.cs6240.utils.reducer.ExecuteReduce;

/**
 * Handles all the reduce tasks sent to the Slave by the master.This processes
 * the incoming data only if the the object is of type
 * {@link neu.mr.cs6240.sharedobjects.Task} and
 * {@link neu.mr.cs6240.sharedobjects.TaskType.REDUCE_TASK} else sends the
 * object to the next handler
 * 
 * @author ajay subramanya
 *
 */
public class ReduceHandler extends SimpleChannelInboundHandler<Object> {
	SlaveStateMachine state = SlaveStateMachine.getInstance();
	Task task;
	final Logger logger = Logger.getLogger(ReduceHandler.class);

	@Override
	public boolean acceptInboundMessage(Object msg) throws Exception {
		if (!(msg instanceof Task)) return false;
		task = (Task) msg;
		if (task.getTaskType() == TaskType.REDUCE_TASK) return true;
		return false;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
		logger.info("in reduce handler");
		state.setTsk(task.getTaskType());
		S3IO.downloadFiles(task);
		ExecuteReduce.start(task);
		if (state.getErrCode() == null) state.setErrCode(ERR_CODE.REDUCE_SUCCESS);

		TaskResult res = new TaskResult(task.getTaskId(), task.getTaskType(), state.getErrCode());
		ctx.writeAndFlush(res);
		state.setErrCode(null);
	}

}
