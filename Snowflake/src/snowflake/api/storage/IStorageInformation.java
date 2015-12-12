package snowflake.api.storage;

import java.math.BigInteger;

import j3l.util.close.IStateClosure;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.05_0
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
	BigInteger getUsedSpace();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	BigInteger getFreeSpace();
	
	
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
	long getNumberOfChunks();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	double getAverageChunkSize();
	
}
