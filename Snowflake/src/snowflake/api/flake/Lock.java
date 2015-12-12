package snowflake.api.flake;

import j3l.util.check.ArgumentChecker;
import snowflake.core.flake.FlakeLockManager;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.12_0
 * @author Johannes B. Latzel
 */
public final class Lock {
	
	
	/**
	 * <p></p>
	 */
	private final FlakeLockManager flake_lock_manager;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_valid;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Lock(FlakeLockManager flake_lock_manager) {
		ArgumentChecker.checkForNull(flake_lock_manager, "flake_lock_manager");
		this.flake_lock_manager = flake_lock_manager;
		is_valid = true;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean isValid() {
		return is_valid;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void releaseLock() {
		if( isValid() ) {
			flake_lock_manager.releaseLock();
			is_valid = false;
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public String getOwner() {
		return flake_lock_manager.getOwner();
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#finalize()
	 */
	@Override public void finalize() {
		releaseLock();
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		return "Lock by \"" + getOwner() + "\"";
	}
	
}
