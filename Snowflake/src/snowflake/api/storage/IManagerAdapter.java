package snowflake.api.storage;

import java.util.stream.Stream;

import j3l.util.stream.StreamMode;
import snowflake.api.chunk.IChunkInformation;
import snowflake.api.flake.IFlake;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.05_0
 * @author Johannes B. Latzel
 */
public interface IManagerAdapter {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	IFlake declareFlake(long identification);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	IFlake createFlake();
	
	
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
	 
	 
	 /**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	 void mergeAvailableChunks(int number_of_attempts);
	 
	 
	 /**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	 Stream<IFlake> getFlakes(StreamMode stream_mode);
	 
	 
	 /**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	 Stream<IChunkInformation> getAvailableChunks(StreamMode stream_mode);
	
}
