package snowflake.core.manager;

import snowflake.core.Channel;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.04.07_0
 * @author Johannes B. Latzel
 */
public interface IChannelManager extends IReturnChannel {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	Channel getChannel();
	
}
