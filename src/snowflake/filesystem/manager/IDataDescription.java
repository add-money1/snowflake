package snowflake.filesystem.manager;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.19_0
 * @author Johannes B. Latzel
 */
public interface IDataDescription {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	byte getDeduplicationLevel();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getEndOfDeduplicationPointer();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	boolean isDeduplicated();

}
