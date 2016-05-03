package neu.mr.cs6240.common;
/**
 * Class is used to check for SanityFailures for each dataline parsed
 * @author smitha bangalore naresh
 * @version 1.0
 * */

public class SanityFailedException extends Exception {

	private static final long serialVersionUID = 1L;

	public SanityFailedException(String message) {
        super(message);
    }
}