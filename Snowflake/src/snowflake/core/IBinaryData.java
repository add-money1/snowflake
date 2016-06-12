package snowflake.core;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.04_0
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
	default byte[] getBinaryData() {
		byte[] buffer = new byte[getDataLength()];
		getBinaryData(buffer);
		return buffer;
	}
	
	
	/**
	 * <p>must be constant-expression!</p>
	 *
	 * @param
	 * @return
	 */
	public int getDataLength();
	
}
