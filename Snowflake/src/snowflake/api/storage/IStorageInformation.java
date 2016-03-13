package snowflake.api.storage;

import j3l.util.close.IStateClosure;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.03.11_0
 * @author Johannes B. Latzel
 */
public interface IStorageInformation extends IStateClosure {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getAllocatedSpace();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getUsedSpace();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getFreeSpace();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	int getNumberOfFlakes();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	int getNumberOfDamagedFlakes();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	double getAverageFlakeSize();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	double getAverageNumberOfChunksPerFlake();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	default long getNumberOfChunks() {
		long chunks = getNumberOfUsedChunks() + getNumberOfFreeChunks();
		if( chunks < 0 ) {
			return Long.MAX_VALUE;
		}
		return chunks;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getNumberOfUsedChunks();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getNumberOfFreeChunks();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	double getAverageChunkSize();
	
}
