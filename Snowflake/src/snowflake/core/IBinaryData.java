package snowflake.core;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.05.04_0
 * @author Johannes B. Latzel
 */
public interface IBinaryData {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void getBinaryData(byte[] buffer);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public int getDataLength();
	
}
