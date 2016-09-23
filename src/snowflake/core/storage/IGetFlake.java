package snowflake.core.storage;

import snowflake.api.IFlake;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.23_0
 * @author Johannes B. Latzel
 */
public interface IGetFlake {
	
	
	IFlake getFlake(long flake_identification);

}
