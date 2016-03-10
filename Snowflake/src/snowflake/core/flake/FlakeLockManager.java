package snowflake.core.flake;

import j3l.util.check.ArgumentChecker;
import snowflake.api.GlobalString;
import snowflake.api.flake.Lock;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.03.10_0
 * @author Johannes B. Latzel
 */
public final class FlakeLockManager {
	
	
	/**
	 * <p></p>
	 */
	private Object lock_owner;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_locked;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FlakeLockManager() {
		lock_owner = null;
		is_locked = false;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Lock lock(Object owner) {
		if( isLocked() ) {
			throw new SecurityException("The instance is currently locked by: \"" + lock_owner.toString() + "\".");
		}
		lock_owner = ArgumentChecker.checkForNull(owner, GlobalString.Owner.toString());
		is_locked = true;
		return new Lock(this);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean isLocked() {
		return is_locked;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public String getOwner() {
		if( !isLocked() ) {
			return "";
		}
		return lock_owner.toString();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void releaseLock() {
		lock_owner = null;
		is_locked = false;
	}
	
	
}
