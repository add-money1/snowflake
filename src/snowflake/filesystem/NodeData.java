package snowflake.filesystem;

import j3l.util.IBinaryData;
import j3l.util.check.ArgumentChecker;
import snowflake.GlobalString;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.18_0
 * @author Johannes B. Latzel
 */
public abstract class NodeData implements IBinaryData {
	
	
	/**
	 * <p></p>
	 */
	private final long index;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	protected NodeData(long index) {
		this.index = ArgumentChecker.checkForBoundaries(index, 0, Long.MAX_VALUE, GlobalString.Index.toString());
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final long getIndex() {
		return index;
	}
	
}
