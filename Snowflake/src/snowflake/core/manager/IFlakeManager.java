package snowflake.core.manager;

import j3l.util.close.IStateClosure;
import snowflake.api.IFlake;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.05.03_0
 * @author Johannes B. Latzel
 */
public interface IFlakeManager extends IStateClosure  {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	IFlake createFlake(ChunkManager chunk_manager);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	IFlake getFlake(long indentification);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	boolean flakeExists(long identification);

}
