package snowflake.api.storage;

import java.util.EventListener;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.11.16_0
 * @author Johannes B. Latzel
 */
public interface IListenerAdapter {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void addListener(EventListener event_listener);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void removeListener(EventListener event_listener);
	
}
