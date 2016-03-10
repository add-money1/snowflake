package snowflake.api;

import j3l.util.check.ArgumentChecker;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.03.10_0
 * @author Johannes B. Latzel
 */
public final class TableMember<T extends IBinaryData> {
	
	
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
	public TableMember(T binary_data, long table_index) {
		this.binary_data = ArgumentChecker.checkForNull(binary_data, GlobalString.BinaryData.toString());
		this.table_index = ArgumentChecker.checkForBoundaries(
			table_index, 0, Long.MAX_VALUE, GlobalString.TableIndex.toString()
		);
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
		if( object != null && object instanceof TableMember<?> ) {
			return ((TableMember<?>)object).getBinaryData().equals(getBinaryData());			
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
