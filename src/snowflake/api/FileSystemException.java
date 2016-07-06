package snowflake.api;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.11_0
 * @author Johannes B. Latzel
 */
public class FileSystemException extends RuntimeException {
	
	
	/**
	 * <p></p>
	 */
	private static final long serialVersionUID = 3286289478800867549L;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FileSystemException(String message) {
		this(message, null);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FileSystemException(String message, Throwable source) {
		super(message, source);
	}

}
