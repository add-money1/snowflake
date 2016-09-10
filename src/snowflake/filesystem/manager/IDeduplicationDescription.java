package snowflake.filesystem.manager;

import snowflake.api.IAttributeValue;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.10_0
 * @author Johannes B. Latzel
 */
public interface IDeduplicationDescription extends IAttributeValue<IDeduplicationDescription> {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getEndOfDeduplicationPointer();

}
