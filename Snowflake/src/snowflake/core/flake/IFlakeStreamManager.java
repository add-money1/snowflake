package snowflake.core.flake;

import j3l.util.close.IStateClosure;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.05_0
 * @author Johannes B. Latzel
 */
public interface IFlakeStreamManager {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void removeStream(IStateClosure closeable_stream);
	
}
