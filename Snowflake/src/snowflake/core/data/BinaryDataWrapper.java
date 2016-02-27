package snowflake.core.data;

import j3l.util.check.ArgumentChecker;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.02.23_0
 * @author Johannes B. Latzel
 */
public final class BinaryDataWrapper<T extends IBinaryData> {
	
	
	/**
	 * <p></p>
	 */
	private final long table_index;
	
	
	/**
	 * <p></p>
	 */
	private final IBinaryData binary_data;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */ 
	public BinaryDataWrapper(T binary_data, long table_index) {
		this.binary_data = ArgumentChecker.checkForNull(binary_data, "binary_data");
		this.table_index = ArgumentChecker.checkForBoundaries(table_index, 0, Long.MAX_VALUE, "table_index");
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getTableIndex() {
		return table_index;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public IBinaryData getBinaryData() {
		return binary_data;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override public boolean equals(Object object) {
		if( object != null && object instanceof BinaryDataWrapper<?> ) {
			return ((BinaryDataWrapper<?>)object).getBinaryData().equals(getBinaryData());			
		}
		else {
			return false;
		}
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override public int hashCode() {
		return binary_data.hashCode();
	}

}
