package snowflake.api;

import snowflake.filesystem.Lock;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.15_0
 * @author Johannes B. Latzel
 */
public interface ILock {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Lock lock();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void unlock(Lock lock);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean isLocked();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	boolean isLockedBy(Lock lock);
	
}
