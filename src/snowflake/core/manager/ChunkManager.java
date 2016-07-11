package snowflake.core.manager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;

import javax.xml.stream.StreamFilter;

import j3l.util.Checker;
import j3l.util.ClosureState;
import j3l.util.ComparisonType;
import j3l.util.IClose;
import j3l.util.LoopedTaskThread;
import j3l.util.RedundantBinaryTree;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.api.IStorageInformation;
import snowflake.api.StorageException;
import snowflake.core.Chunk;
import snowflake.core.ChunkData;
import snowflake.core.ChunkUtility;
import snowflake.core.DataTable;
import snowflake.core.Flake;
import snowflake.core.IChunk;
import snowflake.core.SplitChunk;
import snowflake.core.TableMember;
import snowflake.core.storage.IAllocateSpace;
import snowflake.core.storage.IChunkManagerConfiguration;
import snowflake.core.storage.IClearChunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.11_0
 * @author Johannes B. Latzel
 */
public final class ChunkManager implements IChunkManager, IChunkMemory, IClose<IOException> {
	
	
	/**
	 * <p></p>
	 */
	public final static ChunkData NULL_CHUNK_DATA = new ChunkData(0, 0, 0, 0, (byte)0);
	
	
	/**
	 * <p></p>
	 */
	private final ChunkMergingManager chunk_merging_manager;
	
	
	/**
	 * <p></p>
	 */
	private ClosureState closure_state;
	
	
	/**
	 * <p></p>
	 */
	private final DataTable<ChunkData> data_table;
	
	
	/**
	 * <p></p>
	 */
	private final IChunkManagerConfiguration chunk_manager_configuration;
	
	
	/**
	 * <p></p>
	 */
	private final RedundantBinaryTree<Chunk, Long> available_chunk_tree;
	
	
	/**
	 * <p></p>
	 */
	private final IStorageInformation storage_information;
	
	
	/**
	 * <p></p>
	 */
	private final IAllocateSpace allocate_space;
	
	
	/**
	 * <p></p>
	 */
	private final ChunkRecyclingManager chunk_recycling_manager;
	
	
	/**
	 * <p></p>
	 */
	private final LoopedTaskThread chunk_manager_thread;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public ChunkManager(IStorageInformation storage_information, IClearChunk clear_chunk, 
			IChunkManagerConfiguration chunk_manager_configuration, IAllocateSpace allocate_space) {
		if( StaticMode.TESTING_MODE ) {
			this.chunk_manager_configuration = Checker.checkForNull(
				chunk_manager_configuration, GlobalString.ChunkManagerConfiguration.toString()
			);
			this.storage_information = Checker.checkForNull(
				storage_information, GlobalString.StorageInformation.toString()
			);
			this.allocate_space = Checker.checkForNull(allocate_space, GlobalString.AllocateSpace.toString());
			File chunk_table_file = new File(chunk_manager_configuration.getChunkTableFilePath());
			data_table = new DataTable<>(Checker.checkForExistence(
				chunk_table_file, GlobalString.ChunkTableFile.toString()
			));
		}
		else {
			this.chunk_manager_configuration = chunk_manager_configuration;
			this.storage_information = storage_information;
			this.allocate_space = allocate_space;
			this.data_table = new DataTable<>(new File(chunk_manager_configuration.getChunkTableFilePath()));
		}
		chunk_merging_manager = new ChunkMergingManager(this);
		chunk_recycling_manager = new ChunkRecyclingManager(
			clear_chunk, chunk_manager_configuration.getChunkRecyclingTreshhold()
		);
		available_chunk_tree = new RedundantBinaryTree<>(chunk -> new Long(chunk.getLength()));
		closure_state = ClosureState.None;
		chunk_manager_thread = new LoopedTaskThread(this::manage, "Snowflake ChunkManagerThread", 61_000);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void manage() {
		chunk_merging_manager.addAll(chunk_recycling_manager.removeAll());
		data_table.trim();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void addAvailableChunk(Chunk chunk) {
		if( StaticMode.TESTING_MODE ) {
			if( !isOpen() && !isInOpening() ) {
				throw new SecurityException("The instance is not open!");
			}
		}
		Checker.checkForValidation(chunk, GlobalString.Chunk.toString());
		synchronized( available_chunk_tree ) {
			if( !available_chunk_tree.add(chunk) ) {
				throw new SecurityException("A chunk got lost on its way!");
			}
			if( available_chunk_tree.getSize() > chunk_manager_configuration.getMaximumAvailableChunks() ) {
				if( !chunk_merging_manager.addAll(
						available_chunk_tree.removeSome(
							available_chunk_tree.getSize() - chunk_manager_configuration.getMaximumAvailableChunks()
						)
					)
				) {
					throw new SecurityException("A chunk got lost on its way!");
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
	public void addAvailableChunks(ArrayList<Chunk> chunk_list) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(chunk_list, GlobalString.ChunkList.toString());
		}
		if( chunk_list.isEmpty() ) {
			return;
		}
		for( Chunk chunk : chunk_list ) {
			if( chunk == null || !chunk.isValid() ) {
				throw new StorageException("Can not add the chunk " + chunk + ": it's either null or invalid!");
			}
		}
		synchronized( available_chunk_tree ) {
			if( !available_chunk_tree.addAll(chunk_list) ) {
				throw new SecurityException("A chunk got lost on its way!");
			}
			if( available_chunk_tree.getSize() > chunk_manager_configuration.getMaximumAvailableChunks() ) {
				if( !chunk_merging_manager.addAll(
						available_chunk_tree.removeSome(
							available_chunk_tree.getSize() - chunk_manager_configuration.getMaximumAvailableChunks()
						)
					)
				) {
					throw new SecurityException("A chunk got lost on its way!");
				}
			}
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	private void createAvailableChunk(long minimum_length) {
		
		if( StaticMode.TESTING_MODE ) {
			if( !isOpen() ) {
				throw new SecurityException("The ChunkManager is not open!");
			}
		}
		
		chunk_merging_manager.addAll(chunk_recycling_manager.removeAll());
		Chunk chunk = chunk_merging_manager.getChunk(minimum_length);
		
		if( chunk == null ) {
			
			ChunkData temporary_chunk_data;
			long allocated_space;
			long chunk_length;
			long additional_length;
			long available_index;
			
			synchronized( available_chunk_tree ) {
				
				allocated_space = storage_information.getAllocatedSpace();
				additional_length = (long)( allocated_space * chunk_manager_configuration.getDataFileIncreaseRate() );
				
				if( additional_length < 0 ) {
					throw new StorageException("The additional_length of this method managed to overflow! :o");
				}
				
				chunk_length = minimum_length + additional_length;
				
				if( chunk_length < chunk_manager_configuration.getPreferredAvailableStorageSize() + minimum_length ) {
					chunk_length = chunk_manager_configuration.getPreferredAvailableStorageSize() + minimum_length;
				}
				
				temporary_chunk_data = allocate_space.allocateSpace(chunk_length);
				
			}
			
			
			available_index = data_table.getAvailableIndex();
			chunk = new Chunk(this, temporary_chunk_data.getStartAddress(), 
					temporary_chunk_data.getChunkLength(), available_index);
			chunk.save(null);
			
		}
		
		addAvailableChunk(chunk);
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Stream<IChunk> streamAvailableChunks(StreamMode stream_mode) {
		synchronized( available_chunk_tree ) {
			return Stream.concat(
				available_chunk_tree.stream(stream_mode).filter(StreamFilter::filterNull).<IChunk>map(_o->_o),
				chunk_merging_manager.getStream(stream_mode)
			);
		}
	}


	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setInitialIndices(ArrayList<Long> available_index_list) {
		if( StaticMode.TESTING_MODE ) {
			if( hasBeenOpened() ) {
				throw new SecurityException("The chunk_manager must not be opened when the initial indices are set!");
			}
			Checker.checkForNull(available_index_list, GlobalString.AvailableIndexList.toString());
		}
		if( !available_index_list.isEmpty() ) {
			data_table.addAvailableIndices(available_index_list);
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private Chunk trimToSizeUnsafe(Chunk chunk, long size) {
		if( chunk.isPartOfFlake() ) {
			throw new SecurityException("Do not ever trim a chunk unsafe when the chunk is part of a flake!");
		}
		SplitChunk split_chunk = splitChunk(chunk, size);
		addAvailableChunk(split_chunk.getRightChunk());
		return split_chunk.getLeftChunk();
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
		
		if( StaticMode.TESTING_MODE ) {
			if( hasBeenOpened() ) {
				return;
			}
		}

		closure_state = ClosureState.InOpening;
		chunk_recycling_manager.start();
		chunk_merging_manager.start();
		chunk_manager_thread.start();
		closure_state = ClosureState.Open;
		
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IClose#close()
	 */
	@Override public void close() throws IOException {
		
		if( StaticMode.TESTING_MODE ) {
			if( !isOpen() ) {
				return;
			}
		}

		closure_state = ClosureState.InClosure;
		chunk_recycling_manager.stop();
		chunk_merging_manager.stop();
		chunk_manager_thread.interrupt();
		closure_state = ClosureState.Closed;
		
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkMemory#deleteChunk(snowflake.core.Chunk)
	 */
	@Override public void deleteChunk(Chunk chunk) {
		Checker.checkForValidation(chunk, GlobalString.Chunk.toString());
		data_table.addEntry(new TableMember<>(ChunkManager.NULL_CHUNK_DATA, chunk.getChunkTableIndex()));
		data_table.addAvailableIndex(chunk.getChunkTableIndex());
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkMemory#saveChunk(snowflake.core.Chunk)
	 */
	@Override public void saveChunk(Flake owner_flake, Chunk chunk) {
		data_table.addEntry(new TableMember<>(ChunkUtility.getChunkData(owner_flake, chunk), 
				chunk.getChunkTableIndex()));
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkManager#recycleChunk(snowflake.core.Chunk)
	 */
	@Override public void recycleChunk(Chunk chunk) {
		if( !chunk_recycling_manager.add(chunk) ) {
			throw new StorageException("Could not add " + chunk.toString() + " to the chunk_recycling_manager!");
		}
	}


	/* (non-Javadoc)
	 * @see snowflake.api.chunk.IChunkManager#recycleChunks(java.util.Collection)
	 */
	@Override public void recycleChunks(Collection<Chunk> chunk_collection) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(chunk_collection, GlobalString.ChunkCollection.toString());
		}
		if( chunk_collection.size() > 0 ) {
			if( !chunk_recycling_manager.addAll(chunk_collection) ) {
				throw new StorageException("Could not add all chunks to the chunk_recycling_manager!");
			}
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkManager#mergeChunks(java.util.Collection)
	 */
	@Override public Chunk mergeChunks(Collection<Chunk> chunk_collection) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(chunk_collection, GlobalString.Chunks.toString());
		}
		Chunk[] chunks = chunk_collection.toArray(new Chunk[0]);
		if( chunks.length == 0 ) {
			throw new IllegalArgumentException("The length of the chunk must not be equal to 0!");
		}
		
		if( chunks.length == 1 ) {
			return chunks[0];
		}
		
		
		//tests all but the last chunk if the next chunk is their neighbour 
		for(int a=0,n=chunks.length;a<n-1;a++) {
			
			if( chunks[a] == null || chunks[a+1] == null ) {
				throw new NullPointerException("Either chunks[" + a + "] or chunks[" + (a + 1) + "] is null!");
			}
			
			if( !chunks[a].isNeighbourOf(chunks[a+1]) ) {
				throw new IllegalArgumentException("Not all elements in chunks are neighbours "
						+ "of their precesser and their successer!");
			}
			
		}
		
		
		long position_in_flake = chunks[0].getPositionInFlake();
		long length = 0;
		long start_address = chunks[0].getStartAddress();
		
		for( Chunk chunk : chunks ) {
			
			length += chunk.getLength();
			
			if( chunk.getPositionInFlake() < position_in_flake ) {
				position_in_flake = chunk.getPositionInFlake();
			}
			
			if( chunk.getStartAddress() < start_address ) {
				start_address = chunk.getStartAddress();
			}
			
			chunk.delete();
			
		}
		
		
		Chunk merged_chunk = new Chunk(this, start_address, length, data_table.getAvailableIndex());
		merged_chunk.setPositionInFlake(position_in_flake);
		merged_chunk.save(null);
		
		return merged_chunk;
		
		
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkManager#splitChunk(snowflake.core.Chunk, long)
	 */
	@Override public SplitChunk splitChunk(Chunk chunk, long position) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(chunk, GlobalString.Chunk.toString());
		}
		if( chunk.getLength() == 1 ) {
			throw new IllegalArgumentException("A chunk of length 1 can not be splitted.");
		}
		Checker.checkForBoundaries(position, 1, chunk.getLength() - 1, GlobalString.Position.toString());
		SplitChunk split_chunk = new SplitChunk(
			new Chunk(
				this, chunk.getStartAddress(), position, data_table.getAvailableIndex()
			),
			new Chunk(
				this, chunk.getStartAddress() + position, chunk.getLength() - position, data_table.getAvailableIndex()
			)
		);
		if( chunk.getPositionInFlake() >= 0 ) {
			split_chunk.getLeftChunk().setPositionInFlake(chunk.getPositionInFlake());
			split_chunk.getRightChunk().setPositionInFlake(chunk.getPositionInFlake() + position);
		}
		chunk.delete();
		split_chunk.getLeftChunk().save(null);
		split_chunk.getRightChunk().save(null);
		return split_chunk;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkManager#trimToSize(snowflake.core.Chunk, long)
	 */
	@Override public Chunk trimToSize(Chunk chunk, long size) {
		SplitChunk split_chunk = splitChunk(chunk, size);
		recycleChunk(split_chunk.getRightChunk());
		return split_chunk.getLeftChunk();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.manager.IChunkManager#allocateSpace(long)
	 */
	@Override public Collection<Chunk> allocateSpace(long number_of_bytes) {
		if( StaticMode.TESTING_MODE ) {
			if( !isOpen() ) {
				throw new SecurityException("The instance is not open!");
			}
			Checker.checkForBoundaries(
				number_of_bytes, 1, Long.MAX_VALUE, GlobalString.NumberOfBytes.toString()
			);
		}
		ArrayList<Chunk> chunk_list = new ArrayList<>(1);
		Chunk current_chunk;
		long remaining_bytes = number_of_bytes;
		long current_available_chunk_list_size;
		synchronized( available_chunk_tree ) {
			do {
				current_available_chunk_list_size = available_chunk_tree.getSize();
				if( current_available_chunk_list_size > 0 ) {
					current_chunk = available_chunk_tree.remove(
						ComparisonType.SmallerThanOrEqualTo, new Long(remaining_bytes)
					);
					if( current_chunk != null ) {
						remaining_bytes -= current_chunk.getLength();
						chunk_list.add(current_chunk);
					}
					else {
						break;
					}
				}
				else {
					break;
				}
			}
			while( remaining_bytes > 0 );
		}
		if( remaining_bytes > 0 ) {
			do {
				synchronized( available_chunk_tree ) {
					current_chunk = available_chunk_tree.remove(
						ComparisonType.GreaterThanOrEqualTo, new Long(remaining_bytes)
					);
				}
				if( current_chunk != null ) {
					if( current_chunk.getLength() <= remaining_bytes ) {
						remaining_bytes -= current_chunk.getLength();
						chunk_list.add(current_chunk);
					}
					else {
						// trimToSizeUnsafe() is okay, because the chunk has already been available
						chunk_list.add(trimToSizeUnsafe(current_chunk, remaining_bytes));
						remaining_bytes = 0;
					}
					current_chunk = null;
				}
				else {
					createAvailableChunk(remaining_bytes);
				}
			}
			while( remaining_bytes > 0 );
		}
		return chunk_list;
	}
	
}
