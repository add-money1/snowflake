package snowflake.core.data;

import j3l.util.check.ArgumentChecker;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.11.23_0
 * @author Johannes B. Latzel
 */
public class TableMember {
	
	
	/**
	 * <p></p>
	 */
	private final long table_index;
	
	
	/**
	 * <p></p>
	 */
	private final IBinaryData binary_member;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	protected TableMember(IBinaryData binary_member, long table_index) {

		ArgumentChecker.checkForNull(binary_member, "binary_member");
		ArgumentChecker.checkForBoundaries(table_index, 0, Long.MAX_VALUE, "table_index");
		
		this.binary_member = binary_member;
		this.table_index = table_index;
		
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
	 */
	public void getBinaryData(byte[] buffer) {
		binary_member.getBinaryData(buffer);
	}

}
