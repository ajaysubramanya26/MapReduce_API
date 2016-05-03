package neu.mr.cs6240.mapred;

/**
 * @author Swapnil Mahajan
 * @author Prasad Memane
 * @author ajay subramanya
 */

/**
 * 
 * This class holds the customization variables used by this MapReduce framework
 */
public class Customizations {
	public static final String keyValSep = "\t";
	public static final int noWrite = 0;
	public static final int noRead = 0;
	public static final String redOutPattern = "part-r-";
	public static final String mapIntPath = "MAP_TASK_";
	public static final String redOutPath = "REDUCE_TASK_";
	public static final String redInpPath = "_ReduceInputs_";
	public static final String mapOutPattern = "__MAPTASK";
	public static final String encoding = "utf-8";
	public static final String zipExt = ".gz";
	public static final char padChar = '0';
}
