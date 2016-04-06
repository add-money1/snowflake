package snowflake.api.flake;

import j3l.util.close.IStateClosure;
import snowflake.core.manager.ChunkManager;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.03.14_0
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
