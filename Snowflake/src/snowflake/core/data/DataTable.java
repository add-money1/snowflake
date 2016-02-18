package snowflake.core.data;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

import j3l.exception.ClosureException;
import j3l.util.check.ArgumentChecker;
import j3l.util.check.ClosureChecker;
import j3l.util.close.ClosureState;
import j3l.util.close.IClose;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.24_0
 * @author Johannes B. Latzel
 */
public final class DataTable<T extends IBinaryData> implements IClose<IOException> {
	
	
	/**
	 * <p></p>
	 */
	private final LinkedList<BinaryDataWrapper<T>> pending_entries_list;
	
	
	/**
	 * <p></p>
	 */
	private final LinkedList<BinaryDataWrapper<T>> pending_entries_buffer_list;
	
	
	/**
	 * <p></p>
	 */
	private final File table_file;
	
	
	/**
	 * <p></p>
	 */
	private final int max_capacity;
	
	
	/**
	 * <p></p>
	 */
	private ClosureState closure_state;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_flushing;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public DataTable(File table_file, int max_capacity) {
		
		ArgumentChecker.checkForNull(table_file, "table_file");
		ArgumentChecker.checkForBoundaries(max_capacity, 1, Integer.MAX_VALUE, "max_capacity");
		
		
		if( !table_file.isFile() ) {
			throw new IllegalArgumentException("The table_file must exist and be a file!");
		}
		
		this.table_file = table_file;
		this.max_capacity = max_capacity;		
		
		pending_entries_list = new LinkedList<>();
		pending_entries_buffer_list = new LinkedList<>();
		closure_state = ClosureState.None;
		is_flushing = false;
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	public void flush() throws IOException {
		
		if( !hasBeenOpened() || isClosed() || isFlushing() ) {
			return;
		}
		
		is_flushing = true;
		byte[] buffer;
		IBinaryData current_entry;
		BinaryDataWrapper<T> current_wrapper = null;
		
		synchronized( pending_entries_list ) {
			
			if( pending_entries_list.size() != 0 ) {
				
				
				buffer = new byte[pending_entries_list.get(0).getBinaryData().getDataLength()];
				
				try (RandomAccessFile table = new RandomAccessFile(table_file, "rw")) {
					
					do {

						current_wrapper = pending_entries_list.getFirst();
						current_entry = current_wrapper.getBinaryData();
						current_entry.getBinaryData(buffer);
						
						table.seek( buffer.length * current_wrapper.getTableIndex() );
						table.write(buffer);
						
						pending_entries_list.removeFirst();
						
					}
					while( pending_entries_list.size() > 0 );
					
				}
				catch( IOException e ) {
					
					if( current_wrapper != null ) {
						pending_entries_list.addFirst(current_wrapper);
					}
					
					throw new IOException("Could not flush the data in this table! Stopped at wrapper \""
							+ (current_wrapper == null ? "null" : current_wrapper.toString()) + "\".", e);
				}
				
				
			}
			
		}
		
		is_flushing = false;
		
		transferBufferEntries();
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void transferBufferEntries() {
		
		if( isFlushing() || !hasBeenOpened() ) {
			return;
		}
		
		
		synchronized( pending_entries_buffer_list ) {
			
			if( pending_entries_buffer_list.size() == 0 ) {
				return;
			}
			
			synchronized( pending_entries_list ) {
				do {
					pending_entries_list.add(pending_entries_buffer_list.removeFirst());
				}
				while( pending_entries_buffer_list.size() > 0 );
			}
		}
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private boolean isFlushing() {
		return is_flushing;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	public void addEntry(BinaryDataWrapper<T> wrapper) {
		
		if( !hasBeenOpened() ) {
			throw new ClosureException("The table has not been opened!");
		}
		else {
			ClosureChecker.checkForOpen(this, "table");
		}
		
		
		if( wrapper != null ) {
			if( isFlushing() ) {
				synchronized( pending_entries_buffer_list ) {
					pending_entries_buffer_list.addLast(wrapper);
				}
			}
			else {
				synchronized( pending_entries_list ) {
					pending_entries_list.addLast(wrapper);
					if( pending_entries_list.size() >= max_capacity ) {
						flushParallel();
					}
				}
			}
		}		
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public synchronized void flushParallel() {
		
		//System.out.println("DataTable.addEntry() -> flush sollte nicht mit try eingwickelt werden!");
		Thread flush_thread = new Thread(() -> {
			try {
				flush();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		flush_thread.setName("Snowflake DataTable Flush-Thread");
		flush_thread.start();
		
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IStateClosure#getClosureState()
	 */
	@Override public ClosureState getClosureState() {
		return closure_state;
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IClose#open()
	 */
	@Override public void open() throws IOException {
		if( !hasBeenOpened() ) {
			closure_state = ClosureState.Open;
		}
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IClose#close()
	 */
	@Override public void close() throws IOException {
		
		if( !isOpen() ) {
			return;
		}
		
		closure_state = ClosureState.InClosure;
		
		transferBufferEntries();
		flush();
		
		closure_state = ClosureState.Closed;
		
	}

}
