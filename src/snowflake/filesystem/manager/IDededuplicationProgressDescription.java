package snowflake.filesystem.manager;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.23_0
 * @author Johannes B. Latzel
 */
public interface IDededuplicationProgressDescription {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getCurrentIndexPointer();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getBackupFlakeIdentification();
	
}
