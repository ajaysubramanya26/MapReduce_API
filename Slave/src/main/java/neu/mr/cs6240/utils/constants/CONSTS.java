package neu.mr.cs6240.utils.constants;

/**
 * Class with public static member variables to hold the constants that would be
 * used in the slave
 * 
 * @author ajay subramanya
 *
 */
public class CONSTS {
	/**
	 * the directory in which the jar file would be present
	 */
	public static final String JAR_DIR = "_JarDir/";
	/**
	 * the directory in which the map input files would be download'd from s3
	 */
	public static final String MAP_INPUT_DIR = "_MapInputs_";
	/**
	 * Mapper's run method
	 */
	public static final String MAP_RUN_METHOD = "run";
	/**
	 * Mapper's init method
	 */
	public static final String MAP_INIT_METHOD = "init";
	/**
	 * the directory in which the reduce input files would be download'd from s3
	 */
	public static final String REDUCE_INPUT_DIR = "_ReduceInputs_";
	/**
	 * Mapper's run method
	 */
	public static final String REDUCE_RUN_METHOD = "run";
	/**
	 * Mapper's init method
	 */
	public static final String REDUCE_INIT_METHOD = "init";
}
