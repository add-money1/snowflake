package snowflake.core.manager;

import java.util.ArrayList;
import java.util.stream.Stream;

import j3l.util.check.ArgumentChecker;
import j3l.util.collection.ListType;
import j3l.util.collection.SortedList;
import j3l.util.collection.interfaces.add.IAdd;
import j3l.util.stream.IStream;
import j3l.util.stream.StreamFactory;
import j3l.util.stream.StreamFilter;
import j3l.util.stream.StreamMode;
import snowflake.api.storage.StorageException;
import snowflake.core.data.Chunk;
import snowflake.core.storage.IClearChunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.01.20_0
 * @author Johannes B. Latzel
 */
public final class ChunkRecyclingManager implements IStream<Chunk>, IAdd<Chunk> {
	
	
	/**
	 * <p></p>
	 */
	private final SortedList<Long, Chunk> chunk_recycling_list;
	
	
	/**
	 * <p></p>
	 */
	private final ArrayList<Chunk> recycled_chunk_list;
	
	
	/**
	 * <p></p>
	 */
	private final IClearChunk clear_chunk;
	
	
	/**
	 * <p></p>
	 */
	private final long cleaning_treshhold;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public ChunkRecyclingManager(IClearChunk clear_chunk, long cleaning_treshhold) {
		this.clear_chunk = ArgumentChecker.checkForNull(clear_chunk, "clear_chunk");
		this.cleaning_treshhold = ArgumentChecker.checkForBoundaries(cleaning_treshhold, 1, Long.MAX_VALUE, "cleaning_treshhold");
		chunk_recycling_list = new SortedList<>(ListType.ArrayList, chunk -> new Long(chunk.getLength()));
		recycled_chunk_list = new ArrayList<>();
	}
	
	
	/**
	 * <p>recycles one of the chunks in {@link #chunk_recycling_list} and puts it into {@link #recycled_chunk_list}</p>
	 */
	public void recycleChunk() {
		
		Chunk chunk = null;
		
		synchronized( chunk_recycling_list ) {
			if( chunk_recycling_list.isEmpty() ) {
				return;
			}
			else {
				chunk = chunk_recycling_list.remove(chunk_recycling_list.size() / 2);
			}
		}
		
		long remaining_bytes = chunk.getLength();
		long advance;
		while( remaining_bytes > 0 ) {
			advance = cleaning_treshhold < remaining_bytes ? cleaning_treshhold : remaining_bytes;
			try {
				clear_chunk.clearChunk(chunk, chunk.getLength() - remaining_bytes, advance);
			}
			catch( StorageException e ) {
				e.printStackTrace();
				synchronized( chunk_recycling_list ) {
					chunk_recycling_list.add(chunk);
				}
				return;
			}
			remaining_bytes -= advance;
		}
		
		chunk.setNeedsToBeCleared(false);
		chunk.save(null);
		
		synchronized( recycled_chunk_list ) {
			recycled_chunk_list.add(chunk);
		}
		
	}
	
	
	/**
	 * <p>adds a chunk to this manager</p>
	 *
	 * @param chunk the chunk
	 * @return true if the chunk has been added, false otherwise
	 */
	@Override public boolean add(Chunk chunk) {
		if( chunk == null || !chunk.isValid() ) {
			return false;
		}
		else {
			synchronized( chunk_recycling_list ) {
				if( !chunk_recycling_list.contains(chunk) ) {
					return chunk_recycling_list.add(chunk);
				}
				else {
					return false;
				}
			}
		}
	}
	
	
	/**
	 * <p>returns a stream with all recycled chunks</p>
	 *
	 * @param stream_mode the {@link j3l.util.stream.StreamMode}
	 * @return a {@link java.util.stream.Stream} of all recycled Chunks
	 */
	@Override public Stream<Chunk> getStream(StreamMode stream_mode) {
		synchronized( recycled_chunk_list ) {
			Chunk[] elements = recycled_chunk_list.toArray(new Chunk[0]);
			recycled_chunk_list.clear();
			return StreamFactory.getStream(elements, stream_mode);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.collection.interfaces.add.IExtendedAdd#addAll(java.util.stream.Stream)
	 */
	@Override public void addAll(Stream<? extends Chunk> stream) {
		ArgumentChecker.checkForNull(stream, "stream").filter(StreamFilter::filterInvalid).forEach(this::add);
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.collection.interfaces.add.IExtendedAdd#addAll(java.lang.Object[])
	 */
	@Override public boolean addAll(Chunk[] chunk_array) {
		boolean return_value = true;
		for( Chunk chunk : ArgumentChecker.checkForNull(chunk_array, "chunk_array") ) {
			if( StreamFilter.filterInvalid(chunk) ) {
				return_value &= add(chunk);
			}
		}
		return return_value;
	}
	
	
}
