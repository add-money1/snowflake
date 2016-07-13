package snowflake.filesystem;

import java.time.Instant;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.13_0
 * @author Johannes B. Latzel
 */
public final class Lock {
	
	
	/**
	 * <p></p>
	 */
	private final long lock_time_stamp;
	
	
	/**
	 * <p></p>
	 */
	private final Thread lock_thread;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public Lock() {
		lock_time_stamp = Instant.now().toEpochMilli();
		lock_thread = Thread.currentThread();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getLockTimeStamp() {
		return lock_time_stamp;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Thread getLockThread() {
		return lock_thread;
	}
	
}
