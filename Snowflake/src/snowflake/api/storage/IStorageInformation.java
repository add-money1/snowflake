package snowflake.api.storage;

import java.math.BigDecimal;
import java.math.BigInteger;

import j3l.util.close.IStateClosure;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.14_0
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
	BigDecimal getAverageNumberOfChunksPerFlake();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	default BigInteger getNumberOfChunks() {
		return getNumberOfUsedChunks().add(getNumberOfFreeChunks());
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	BigInteger getNumberOfUsedChunks();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	BigInteger getNumberOfFreeChunks();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	BigDecimal getAverageChunkSize();
	
}
