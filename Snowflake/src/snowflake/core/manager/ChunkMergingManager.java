package snowflake.core.manager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Stream;

import j3l.util.IAdd;
import j3l.util.RandomFactory;
import j3l.util.check.ArgumentChecker;
import j3l.util.stream.IStream;
import j3l.util.stream.StreamFactory;
import j3l.util.stream.StreamFilter;
import j3l.util.stream.StreamMode;
import snowflake.api.GlobalString;
import snowflake.api.chunk.IChunkInformation;
import snowflake.api.chunk.IChunkManager;
import snowflake.core.data.Chunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.03.14_0
 * @author Johannes B. Latzel
 */
public final class ChunkMergingManager implements IAdd<Chunk>, IStream<IChunkInformation> {
	
	
	/**
	 * <p></p>
	 */
	private final ArrayList<Chunk> chunk_list;
	
	
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
		this.chunk_manager = ArgumentChecker.checkForNull(chunk_manager, GlobalString.ChunkManager.toString());
		chunk_list = new ArrayList<>();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param nopc_treshold treshold of "number of processed chunks"
	 */
	public void mergeChunks(long nopc_treshold) {
		
		ArrayList<Chunk> available_chunks;
		long actual_nopc_treshold;
		
		synchronized( chunk_list ) {
			long chunk_list_size = chunk_list.size();
			if( chunk_list_size  > 1 ) {
				if( nopc_treshold > chunk_list_size ) {
					actual_nopc_treshold = chunk_list_size;
					available_chunks = new ArrayList<>(chunk_list);
					chunk_list.clear();
				}
				else {
					actual_nopc_treshold = nopc_treshold;
					available_chunks = new ArrayList<>();
					for(int a=0;a<actual_nopc_treshold;a++) {
						available_chunks.add(chunk_list.get(chunk_list.size() - 1));
					}
				}
			}
			else {
				return;
			}
		}
		
		available_chunks.sort((l,r) -> Long.compare(l.getStartAddress(), r.getStartAddress()));
		
		int min_index;
		int max_index;
		int index_difference;
		Random random = RandomFactory.createRandom();
		LinkedList<Chunk> processed_chunk_list = new LinkedList<>();
		LinkedList<Chunk> neighbour_list = new LinkedList<>();
		
		for(int a=0;a<actual_nopc_treshold && available_chunks.size()>1;a++) {
			
			if( !neighbour_list.isEmpty() ) {
				neighbour_list.clear();
			}
			
			min_index = max_index = 1 + random.nextInt(available_chunks.size() - 1);
			
			while( min_index > 0 ) {
				if( available_chunks.get(min_index).isNeighbourOf(available_chunks.get(min_index - 1)) ) {
					min_index--;
				}
				else {
					break;
				}
			}
			
			while( max_index < available_chunks.size() - 1 ) {
				if( available_chunks.get(max_index).isNeighbourOf(available_chunks.get(max_index + 1)) ) {
					max_index++;
				}
				else {
					break;
				}
			}
			
			index_difference = max_index - min_index;
						
			if( index_difference > 0 ) {
				for(int b=0;b<=index_difference;b++) {
					neighbour_list.add(available_chunks.remove(min_index));
				}
			}
			
			if( !neighbour_list.isEmpty() ) {
				processed_chunk_list.add(chunk_manager.mergeChunks(neighbour_list));
			}
			
			
		}
		
		addAll(available_chunks);
		addAll(processed_chunk_list);
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Chunk getChunk(long minimum_length) {
		Predicate<Chunk> filter = chunk -> chunk != null && chunk.getLength() >= minimum_length;
		synchronized( chunk_list ) {
			Chunk available_chunk = chunk_list.parallelStream().filter(filter).findAny().orElse(null);
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
	public Collection<Chunk> removeAll() {
		ArrayList<Chunk> list;
		synchronized( chunk_list ) {
			list = new ArrayList<>(chunk_list);
			chunk_list.clear();
		}
		return list;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean isEmpty() {
		synchronized( chunk_list ) {
			return chunk_list.isEmpty();
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
		synchronized( chunk_list ) {
			if( !chunk_list.contains(chunk) ) {
				return chunk_list.add(chunk);
			}
			else {
				return false;
			}
		}
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.stream.IStream#getStream(j3l.util.stream.StreamMode)
	 */
	@Override public Stream<IChunkInformation> getStream(StreamMode stream_mode) {
		return StreamFactory.getStream(chunk_list, stream_mode)
				.filter(StreamFilter::filterNull).<IChunkInformation>map(_i->_i);
	}
	
}
