package snowflake.core.manager;

import java.io.IOException;

import snowflake.core.Returnable;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.04.07_0
 * @author Johannes B. Latzel
 */
public interface IReturnChannel {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void returnChannel(Returnable channel) throws IOException;
	
}
