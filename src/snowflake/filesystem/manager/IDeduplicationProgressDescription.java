package snowflake.filesystem.manager;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.19_0
 * @author Johannes B. Latzel
 */
public interface IDeduplicationProgressDescription {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getCurrentDataPointer();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getCurrentIndexPointer();
	
}
