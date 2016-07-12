package snowflake.filesystem.manager;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.12_0
 * @author Johannes B. Latzel
 */
public interface IDeduplicationDescription {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public byte getDeduplicationLevel();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getEndOfDeduplicationPointer();

}
