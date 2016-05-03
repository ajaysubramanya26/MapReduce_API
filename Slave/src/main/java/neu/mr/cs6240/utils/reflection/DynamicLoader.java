package neu.mr.cs6240.utils.reflection;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.log4j.Logger;

import neu.mr.cs6240.StateMachine.SlaveStateMachine;
import neu.mr.cs6240.sharedobjects.ERR_CODE;
import neu.mr.cs6240.sharedobjects.TaskType;

/**
 * This class is to load the jar and any Class in the jar and then invoke some
 * methods on the jar. This essentially uses java Reflections. There is only one
 * constructor which needs a jar file. The constructor loads the jar file and is
 * ready to load any valid Class from the jar when loadClass is called. This
 * method loads the class and has the class and an instance of it ready. When
 * the user wants to invoke any method from the class, invokeMethod is called
 * which invokes the method from the class and also passes any necessary
 * arguments that the method may need to run
 * 
 * @author ajay subramanya
 *
 */
public class DynamicLoader {
	private final static Logger logger = Logger.getLogger(DynamicLoader.class);
	private SlaveStateMachine state = SlaveStateMachine.getInstance();
	private File jar;

	private URLClassLoader loadedJar;
	private Class<?> clsToLoad;
	private Object clsInst;

	/**
	 * Constructor
	 * 
	 * @param jar
	 *            the jar file on which we want to load classes and invoke
	 *            methods
	 */
	public DynamicLoader(File jar) {
		this.jar = jar;
		loadJar();
	}

	/**
	 * loads the jar to memory
	 */
	private void loadJar() {
		logger.info("loading jar ");
		try {
			loadedJar = URLClassLoader.newInstance(new URL[] { jar.toURI().toURL() });
		} catch (MalformedURLException e) {
			logger.error("unable to load provided jar " + jar.getName() + "\n" + e.getMessage());
			state.setErrCode(ERR_CODE.UNABLE_TO_LOAD_JAR);
		}
	}

	/**
	 * loads the class to memory and also creates an instance of the class which
	 * we would need while invoking methods
	 * 
	 * @param cls
	 *            the class that we want to load from the jar
	 */
	public void loadClass(String cls) {
		logger.info("loading class " + cls);
		try {
			clsToLoad = loadedJar.loadClass(cls);
			clsInst = clsToLoad.newInstance();
		} catch (ClassNotFoundException e) {
			logger.error("Could not find " + cls + " to load in the jar \n Exception " + e.getLocalizedMessage());
			state.setErrCode(state.getTsk() == TaskType.MAP_TASK ? ERR_CODE.MAP_CLASS_NOT_FOUND
			        : ERR_CODE.REDUCE_CLASS_NOT_FOUND);
		} catch (InstantiationException e) {
			logger.error("Could not instantiate " + cls + " in the jar \n Exception " + e.getLocalizedMessage());
			state.setErrCode(state.getTsk() == TaskType.MAP_TASK ? ERR_CODE.UNABLE_TO_INST_MAP_CLASS
			        : ERR_CODE.UNABLE_TO_INST_REDUCE_CLASS);
		} catch (IllegalAccessException e) {
			logger.error(cls + " is not accessible in the jar \n Exception " + e.getLocalizedMessage());
			state.setErrCode(state.getTsk() == TaskType.MAP_TASK ? ERR_CODE.MAP_CLASS_ILLEGAL_ACCESS
			        : ERR_CODE.REDUCE_CLASS_ILLEGAL_ACCESS);
		}
	}

	/**
	 * invokes the method with the supplied contract and arguments
	 * 
	 * @param method
	 *            the method which we want to invoke
	 * @param params
	 *            the contract of the method
	 * @param args
	 *            the arguments that the method consumes
	 * @return the invoked method's object
	 */
	public Object invokeMethod(String method, Class<?>[] params, Object[] args) {
		Object obj = null;
		try {
			Method md = clsToLoad.getMethod(method, params);
			obj = md.invoke(clsInst, args);
		} catch (NoSuchMethodException e) {
			logger.error(method + " does not exist in the class \n Exception " + e.getLocalizedMessage());
			state.setErrCode(state.getTsk() == TaskType.MAP_TASK ? ERR_CODE.NO_SUCH_METHOD_IN_MAP_CLASS
			        : ERR_CODE.NO_SUCH_METHOD_IN_REDUCE_CLASS);
		} catch (SecurityException e) {
			logger.error(method + " is not accessible in the class " + e.getLocalizedMessage());
			state.setErrCode(state.getTsk() == TaskType.MAP_TASK ? ERR_CODE.MAP_METHOD_SECURITY_EXCEPTION
			        : ERR_CODE.REDUCE_METHOD_SECURITY_EXCEPTION);
		} catch (IllegalAccessException e) {
			logger.error(method + " is not accessible in the class " + e.getLocalizedMessage());
			state.setErrCode(state.getTsk() == TaskType.MAP_TASK ? ERR_CODE.MAP_METHOD_ILLEGAL_ACCESS
			        : ERR_CODE.REDUCE_METHOD_ILLEGAL_ACCESS);
		} catch (IllegalArgumentException e) {
			logger.error("ARGS : " + args + " are not according to the contract of " + method + "\n Exception "
			        + e.getLocalizedMessage());
			state.setErrCode(state.getTsk() == TaskType.MAP_TASK ? ERR_CODE.MAP_METHOD_ILLEGAL_ARG
			        : ERR_CODE.REDUCE_METHOD_ILLEGAL_ARG);
		} catch (InvocationTargetException e) {
			logger.error("Exception thrown from within " + method + "\n" + e.getTargetException().getMessage());
			state.setErrCode(state.getTsk() == TaskType.MAP_TASK ? ERR_CODE.MAP_API_OR_USER_MAPPER_ERROR
			        : ERR_CODE.REDUCE_API_OR_USER_REDUCER_ERROR);
		}
		return obj;
	}

}
