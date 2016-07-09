package snowflake.core.manager;

import j3l.util.check.ArgumentChecker;
import snowflake.GlobalString;
import snowflake.StaticMode;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.09_0
 * @author Johannes B. Latzel
 */
public enum SpecialFlakeIdentification {
	
	FlakeTable(1),
	DirectoryTable(2),
	DeduplicationTable(3);
	
	
	/**
	 * <p></p>
	 */
	private final long identification;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	private SpecialFlakeIdentification(long identification) {
		if( StaticMode.TESTING_MODE ) {
			this.identification = ArgumentChecker.checkForBoundaries(
				identification, 1, Long.MAX_VALUE, GlobalString.Identification.toString()
			);
		}
		else {
			this.identification = identification;
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getIdentification() {
		return identification;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Enum#toString()
	 */
	@Override public String toString() {
		return "SpecialFlakeIdentification [" + identification + "]";
	}
	
}
