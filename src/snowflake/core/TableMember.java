package snowflake.core;

import j3l.util.IBinaryData;
import j3l.util.check.ArgumentChecker;
import snowflake.GlobalString;
import snowflake.StaticMode;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.02_0
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
		if( StaticMode.TESTING_MODE ) {
			this.binary_data = ArgumentChecker.checkForNull(binary_data, GlobalString.BinaryData.toString());
			this.table_index = ArgumentChecker.checkForBoundaries(
				table_index, 0, Long.MAX_VALUE, GlobalString.TableIndex.toString()
			);
		}
		else {
			this.binary_data = binary_data;
			this.table_index = table_index;
		}
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
			IBinaryData extern_binary_data = ((TableMember<?>)object).getBinaryData();
			return 	extern_binary_data == binary_data
					|| ( extern_binary_data != null && extern_binary_data.equals(binary_data));	
		}
		return false;
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override public int hashCode() {
		return binary_data.hashCode();
	}

}
