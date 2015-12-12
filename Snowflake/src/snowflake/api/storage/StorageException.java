package snowflake.api.storage;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.04_0
 * @author Johannes B. Latzel
 */
public class StorageException extends RuntimeException {
	
	
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
	public StorageException(String message) {
		this(message, null);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public StorageException(String message, Throwable source) {
		super(message, source);
	}

}
