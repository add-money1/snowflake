package snowflake.core.manager;

import java.util.LinkedList;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.01.07_0
 * @author Johannes B. Latzel
 */
public final class IndexManager {
	
	
	/**
	 * <p>a list of all available indices</p>
	 */
	private final LinkedList<Long> available_index_list;
	
	
	/**
	 * <p></p>
	 */
	private final Object index_lock;
	
	
	/**
	 * <p></p>
	 */
	private long next_index;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public IndexManager() {
		available_index_list = new LinkedList<>();
		index_lock = new Object();
		next_index = 0;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setNewIndex(long new_index) {
		
		if( new_index < this.next_index ) {
			throw new IllegalArgumentException("The new index must not be smaller than the current index!");
		}
		else {
			if( new_index == this.next_index ) {
				return;
			}
		}
		
		this.next_index = new_index;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getAvailableIndex() {
		
		synchronized( index_lock ) {
			if( available_index_list.size() != 0 ) {
				return available_index_list.removeLast().longValue();
			}
			else {
				long current_index = next_index;
				next_index++;
				return current_index;
			}
		}
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void addAvailableIndex(long index) {
		
		Long boxed_index = new Long(index);
		
		synchronized( index_lock ) {
			if( available_index_list.contains(boxed_index)) {
				throw new Error("An index must never exist more than once!");
			}
			available_index_list.add(boxed_index);
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void clearAvailableIndices() {
		synchronized( index_lock ) {
			available_index_list.clear();
		}
	}
	
}
