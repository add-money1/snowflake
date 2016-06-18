package snowflake.api;

import j3l.util.IBinaryData;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.02_0
 * @author Johannes B. Latzel
 */
public interface IAttributeValue<T> extends IBinaryData {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	T getValue();
	
}
