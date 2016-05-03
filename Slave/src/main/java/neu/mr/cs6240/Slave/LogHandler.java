package neu.mr.cs6240.Slave;

import static neu.mr.cs6240.utils.constants.CONSTS.JAR_DIR;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import neu.mr.cs6240.StateMachine.SlaveStateMachine;
import neu.mr.cs6240.aws.S3;
import neu.mr.cs6240.sharedobjects.ERR_CODE;
import neu.mr.cs6240.sharedobjects.Task;
import neu.mr.cs6240.sharedobjects.TaskResult;
import neu.mr.cs6240.sharedobjects.TaskType;

/**
 * Handles all the log tasks sent to the Slave by the master. this is mainly
 * uploading the logs once we job is success or if something goes wrong
 * 
 * This processes the incoming data only if the the object is of type
 * {@link neu.mr.cs6240.sharedobjects.Task} and
 * {@link neu.mr.cs6240.sharedobjects.TaskType.UPLOAD_LOG} else sends the object
 * to the next handler
 * 
 * @author ajay subramanya
 *
 */
public class LogHandler extends SimpleChannelInboundHandler<Object> {
	SlaveStateMachine state = SlaveStateMachine.getInstance();
	Task task;
	final Logger logger = Logger.getLogger(LogHandler.class);

	@Override
	public boolean acceptInboundMessage(Object msg) throws Exception {
		if (!(msg instanceof Task)) return false;
		task = (Task) msg;
		if (task.getTaskType() == TaskType.UPLOAD_LOG) return true;
		return false;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
		state.setTsk(task.getTaskType());
		try {
			FileUtils.deleteQuietly((new File(JAR_DIR)));
			FileUtils.copyFile(new File("log/Slave.log"), new File("log/Slave" + task.getTaskId() + ".log"));
			S3 s3Obj = new S3();
			String bucketName = s3Obj.getBucketName(task.getOutputPath());
			String prefix = s3Obj.getPrefixName(task.getOutputPath(), bucketName);
			s3Obj.uploadFile(bucketName, prefix + "/" + "Slave" + task.getTaskId() + ".log",
			        new File("log/Slave" + task.getTaskId() + ".log"));

		} catch (IOException e) {}
		if (state.getErrCode() == null) state.setErrCode(ERR_CODE.LOG_SUCCESS);

		TaskResult res = new TaskResult(task.getTaskId(), task.getTaskType(), state.getErrCode());
		ctx.writeAndFlush(res);
	}

}
