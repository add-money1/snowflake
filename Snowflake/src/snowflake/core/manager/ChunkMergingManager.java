package snowflake.core.manager;

import java.util.LinkedList;
import java.util.Random;
import java.util.stream.Stream;

import j3l.util.check.ArgumentChecker;
import j3l.util.collection.ListType;
import j3l.util.collection.SortedList;
import j3l.util.collection.interfaces.add.IAdd;
import j3l.util.random.RandomFactory;
import j3l.util.stream.IStream;
import j3l.util.stream.StreamFilter;
import j3l.util.stream.StreamMode;
import snowflake.api.chunk.IChunkInformation;
import snowflake.api.chunk.IChunkManager;
import snowflake.core.data.Chunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.01.07_0
 * @author Johannes B. Latzel
 */
public final class ChunkMergingManager implements IAdd<Chunk>, IStream<IChunkInformation> {
	
	
	/**
	 * <p></p>
	 */
	private final SortedList<Long, Chunk> chunk_list;
	
	
	/**
	 * <p></p>
	 */
	private final Object chunk_lock;
	
	
	/**
	 * <p></p>
	 */
	private final IChunkManager chunk_manager;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public ChunkMergingManager(IChunkManager chunk_manager) {
		this.chunk_manager = ArgumentChecker.checkForNull(chunk_manager, "chunk_manager");
		chunk_list = new SortedList<>(ListType.LinkedList, chunk -> new Long(chunk.getStartAddress()));
		chunk_lock = new Object();		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param nopc_treshold treshold of "number of processed chunks"
	 */
	public void mergeChunks(int nopc_treshold) {
		
		int actual_nopc_treshold;
		
		synchronized( chunk_lock ) {
			int chunk_list_size = chunk_list.size();
			if( chunk_list_size  > 1 ) {
				if( nopc_treshold > chunk_list_size ) {
					actual_nopc_treshold = chunk_list_size;
				}
				else {
					actual_nopc_treshold = nopc_treshold;
				}
			}
			else {
				return;
			}
		}
		
		
		int min_index;
		int max_index;
		int index_difference;
		Random random = RandomFactory.createRandom();
		LinkedList<Chunk> processed_chunk_list = new LinkedList<>();
		LinkedList<Chunk> neighbour_list = new LinkedList<>();
		
		for(int a=0;a<actual_nopc_treshold;a++) {
			
			if( !neighbour_list.isEmpty() ) {
				neighbour_list.clear();
			}
			
			synchronized( chunk_lock ) {
				
				min_index = max_index = 1 + random.nextInt(chunk_list.size() - 1);
				
				while( min_index > 1 ) {
					if( chunk_list.get(min_index).isNeighbourOf(chunk_list.get(min_index - 1)) ) {
						min_index--;
					}
					else {
						break;
					}
				}
				
				while( max_index < chunk_list.size() - 1 ) {
					if( chunk_list.get(max_index).isNeighbourOf(chunk_list.get(max_index + 1)) ) {
						max_index++;
					}
					else {
						break;
					}
				}
				
				index_difference = (max_index - 1) - (min_index + 1);
				
				if( index_difference > 0 ) {
					for(int b=0;b<index_difference;b++) {
						neighbour_list.add(chunk_list.remove(min_index + 1));
					}
				}
				
			}
			
			if( !neighbour_list.isEmpty() ) {
				processed_chunk_list.add(chunk_manager.mergeChunks(neighbour_list));
			}
			
		}
		
		addAll(processed_chunk_list);
		neighbour_list.clear();
		processed_chunk_list.clear();
		
		System.gc();
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Chunk getChunk(long minimum_length) {
		
		synchronized( chunk_lock ) {
			Chunk available_chunk = chunk_list.getStream(
						StreamMode.Parallel,
						chunk -> chunk != null && chunk.getLength() >= minimum_length
					).findAny().orElse(null);
			
			if( available_chunk != null && !chunk_list.remove(available_chunk) ) {
				return null;
			}
			
			return available_chunk;
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean contains(Chunk chunk) {
		synchronized( chunk_lock ) {
			return chunk_list.contains(chunk);
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see j3l.util.collection.interfaces.add.IBasicAdd#add(java.lang.Object)
	 */
	@Override public boolean add(Chunk chunk) {
		
		if( chunk == null ) {
			return false;
		}
		
		synchronized( chunk_lock ) {
			if( chunk_list.contains(chunk) ) {
				return false;
			}
			return chunk_list.add(chunk);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.stream.IStream#getStream(j3l.util.stream.StreamMode)
	 */
	@Override public Stream<IChunkInformation> getStream(StreamMode stream_mode) {
		synchronized( chunk_lock ) {
			return chunk_list.getStream(stream_mode, StreamFilter::filterNull).<IChunkInformation>map(_i->_i);
		}
	}
	
}
