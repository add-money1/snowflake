package snowflake.core.storage;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.05.03_0
 * @author Johannes B. Latzel
 */
public interface IChannelManagerConfiguration {
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	int getMaximumNumberOfAvailableChannel();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	String getDataFilePath();
	
}
