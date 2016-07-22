package snowflake.filesystem.manager;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.22_0
 * @author Johannes B. Latzel
 */
public interface IDeduplicationDescription {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	int getDeduplicationLevel();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getEndOfDeduplicationPointer();

}
