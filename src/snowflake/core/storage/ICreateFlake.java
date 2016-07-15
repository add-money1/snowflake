package snowflake.core.storage;

import snowflake.api.IFlake;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.15_0
 * @author Johannes B. Latzel
 */
public interface ICreateFlake {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	IFlake createFlake();
	
}
