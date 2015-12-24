package snowflake.core.manager;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.stream.Stream;

import j3l.exception.ValueOverflowException;
import j3l.util.array.ArrayTool;
import j3l.util.check.ArgumentChecker;
import j3l.util.check.ElementChecker;
import j3l.util.close.ClosureState;
import j3l.util.close.IClose;
import j3l.util.collection.ListType;
import j3l.util.collection.SortedList;
import j3l.util.interfaces.ComparisonType;
import j3l.util.stream.StreamFactory;
import j3l.util.stream.StreamFilter;
import j3l.util.stream.StreamMode;
import snowflake.api.chunk.ChunkAppendingMode;
import snowflake.api.chunk.IChunkInformation;
import snowflake.api.chunk.IChunkManager;
import snowflake.api.chunk.IChunkMemory;
import snowflake.api.configuration.IReadOnlyChunkManagerConfiguration;
import snowflake.api.storage.IStorageInformation;
import snowflake.core.data.BinaryDataWrapper;
import snowflake.core.data.Chunk;
import snowflake.core.data.ChunkData;
import snowflake.core.data.ChunkUtility;
import snowflake.core.data.DataTable;
import snowflake.core.flake.Flake;
import snowflake.core.storage.IAllocateSpace;
import snowflake.core.storage.IClearChunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.16_0
 * @author Johannes B. Latzel
 */
public final class ChunkManager implements IChunkManager, IChunkMemory, IClose<IOException> {
	
	
	/**
	 * <p></p>
	 */
	public final static ChunkData NULL_CHUNK_DATA = new ChunkData(0, 0, 0, 0);
	
	
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
	private final IReadOnlyChunkManagerConfiguration chunk_manager_configuration;
	
	
	/**
	 * <p></p>
	 */
	private final SortedList<Long, Chunk> available_chunk_list;
	
	
	/**
	 * <p></p>
	 */
	private final Object available_chunk_lock;
	
	
	/**
	 * <p></p>
	 */
	private final IStorageInformation storage_information;
	
	
	/**
	 * <p></p>
	 */
	private final IClearChunk clear_chunk;
	
	
	/**
	 * <p></p>
	 */
	private final IAllocateSpace allocate_space;
	
	
	/**
	 * <p></p>
	 */
	private final IndexManager index_manager;
	
	
	/**
	 * <p></p>
	 */
	private final Object chunk_file_lock;
	
	
	/**
	 * <p></p>
	 */
	private final File chunk_table_file;
	
	
	/**
	 * <p></p>
	 */
	private final IFlakeModifier flake_modifier;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public ChunkManager(IStorageInformation storage_information, IClearChunk clear_chunk,
			IReadOnlyChunkManagerConfiguration chunk_manager_configuration, IAllocateSpace allocate_space,
			IFlakeModifier flake_modifier) {
		
		ArgumentChecker.checkForNull(chunk_manager_configuration, "chunk_manager_configuration");
		ArgumentChecker.checkForNull(storage_information, "storage_information");
		ArgumentChecker.checkForNull(allocate_space, "allocate_space");
		ArgumentChecker.checkForNull(clear_chunk, "clear_chunk");
		ArgumentChecker.checkForNull(flake_modifier, "flake_modifier");
		
		this.chunk_manager_configuration = chunk_manager_configuration;
		this.storage_information = storage_information;
		this.allocate_space = allocate_space;
		this.clear_chunk = clear_chunk;
		this.flake_modifier = flake_modifier;
		
		chunk_table_file = new File(chunk_manager_configuration.getChunkTableFilePath());
		ArgumentChecker.checkForExistence(chunk_table_file, "chunk_table_file");
		
		data_table = new DataTable<>(new File(chunk_manager_configuration.getChunkTableFilePath()),
				chunk_manager_configuration.getMaximumChunkDataTableSize());
		index_manager = new IndexManager( new File(chunk_manager_configuration.getChunkManagerIndexConfigurationFilePath()) );
		
		chunk_merging_manager = new ChunkMergingManager(this);
		available_chunk_list = new SortedList<>(ListType.LinkedList, chunk -> new Long(chunk.getLength()));
		available_chunk_lock = new Object();
		chunk_file_lock = new Object();
		closure_state = ClosureState.None;
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	private Chunk getAvailableChunk(long number_of_bytes) {
		
		if( !isOpen() ) {
			throw new SecurityException("The instance is not open!");
		}
		
		if( number_of_bytes <= 0 ) {
			throw new IllegalArgumentException("The length must be greater than 0!");
		}
		
		synchronized( available_chunk_lock ) {
			return available_chunk_list.removeAny(ComparisonType.GreaterThanOrEqualTo, new Long(number_of_bytes));
		}
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	private Collection<Chunk> getAvailableChunks(long number_of_bytes) {
		
		if( !isOpen() ) {
			throw new SecurityException("The instance is not open!");
		}
		
		ArgumentChecker.checkForBoundaries(number_of_bytes, 1, Long.MAX_VALUE, "number_of_bytes");
		
		LinkedList<Chunk> chunk_list = new LinkedList<>();
		Chunk current_chunk;
		long remaining_bytes = number_of_bytes;
		int current_available_chunk_list_size;
		
		synchronized( available_chunk_lock ) {
			
			do {
				
				current_available_chunk_list_size = available_chunk_list.size();
				
				if( current_available_chunk_list_size > 0 ) {
					current_chunk = available_chunk_list.removeAny(ComparisonType.SmallerThanOrEqualTo, new Long(remaining_bytes));
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
				current_chunk = getAvailableChunk(remaining_bytes);
				if( current_chunk != null ) {
					if( current_chunk.getLength() <= remaining_bytes ) {
						remaining_bytes -= current_chunk.getLength();
						chunk_list.add(current_chunk);
					}
					else {
						// trimToSizeUnsafe() is okay, because the chunks has already been available
						Chunk trimmed_chunk = trimToSizeUnsafe(current_chunk, remaining_bytes);
						chunk_list.add(trimmed_chunk);
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
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void addAvailableChunk(Chunk chunk) {
		
		if( !isOpen() && !isInOpening() ) {
			throw new SecurityException("The instance is not open!");
		}
		
		if( chunk == null || !chunk.isValid() ) {
			return;
		}
		
		synchronized( available_chunk_lock ) {
			available_chunk_list.add(chunk);
			if( available_chunk_list.size() > chunk_manager_configuration.getMaximumAvailableChunks() ) {
				chunk_merging_manager.add(available_chunk_list.removeLast());
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
		
		if( !isOpen() ) {
			throw new SecurityException("The ChunkManager is not open!");
		}
		
		
		Chunk chunk = chunk_merging_manager.getChunk(minimum_length);
		
		if( chunk == null ) {
			
			ChunkData temporary_chunk_data;
			long allocated_space;
			long chunk_length;
			long additional_length;
			long available_index;
			
			synchronized( available_chunk_lock ) {
				
				allocated_space = storage_information.getAllocatedSpace();
				additional_length = (long)( allocated_space * chunk_manager_configuration.getDataFileIncreaseRate() );
				
				if( additional_length < 0 ) {
					throw new ValueOverflowException("The additional_length of this method managed to overflow! :o");
				}
				
				chunk_length = minimum_length + additional_length;
				
				if( chunk_length < chunk_manager_configuration.getPreferredAvailableStorageSize() + minimum_length ) {
					chunk_length = chunk_manager_configuration.getPreferredAvailableStorageSize() + minimum_length;
				}
				
				temporary_chunk_data = allocate_space.allocateSpace(chunk_length);
				
			}
			
			
			available_index = index_manager.getAvailableIndex();
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
	private void loadChunks() throws IOException {
		
		if( !isInOpening() ) {
			return;
		}
		
		
		LinkedList<Long> declared_flakes_identification_list = new LinkedList<>();
		int current_chunk_number = 0;
		int position_in_input_buffer = 0;
		int max_bytes_in_buffer;
		int bytes_in_buffer;
		long length_of_file;
		long current_position = 0;
		long remaining_bytes_in_file;
		byte[] input_buffer = new byte[ ChunkUtility.BINARY_CHUNK_SIZE * 250 ];
		byte[] chunk_buffer = new byte[ ChunkUtility.BINARY_CHUNK_SIZE ];
		ChunkData chunk_data;
		Chunk chunk;
		Long flake_identificaiton;
		
		
		synchronized( chunk_file_lock ) {
			
			
			length_of_file = chunk_table_file.length();
			
			if( length_of_file == 0 ) {
				return;
			}
			else if( length_of_file % ChunkUtility.BINARY_CHUNK_SIZE != 0 ) {
				throw new SecurityException("The file \"" + chunk_manager_configuration.getChunkTableFilePath()
				+ "\" has been damaged!");
			}
			
			
			try (BufferedInputStream input_stream = new BufferedInputStream(new FileInputStream(chunk_table_file))) {
				
				
				// important, because (int)(length_of_file / ChunkUtility.BINARY_CHUNK_SIZE) could overflow!
				if( length_of_file > (ChunkUtility.BINARY_CHUNK_SIZE * (long)(Integer.MAX_VALUE)) ) {
					throw new IOException("The size of the file \"" + chunk_manager_configuration.getChunkTableFilePath() 
					+ "\" must not be greater than " + (ChunkUtility.BINARY_CHUNK_SIZE * (long)(Integer.MAX_VALUE)) + "!");
				}
				
				
				remaining_bytes_in_file = length_of_file;
				
				while( remaining_bytes_in_file != 0 ) {	
					
					position_in_input_buffer = 0;
					
					if( remaining_bytes_in_file >= input_buffer.length ) {
						max_bytes_in_buffer = input_buffer.length;
					}
					else {
						// cast is okay, because remaining_bytes_in_file is smaller than input_buffer.length which is int
						max_bytes_in_buffer = (int)(remaining_bytes_in_file);						
					}

					bytes_in_buffer = input_stream.read(input_buffer, 0, max_bytes_in_buffer);
					current_position += bytes_in_buffer;
					
					// will always have at least 8 bytes of data and at max buffer.length bytes
					// will always contain a number of bytes which can be divided by 8
					while( position_in_input_buffer != bytes_in_buffer ) {
						
						
						ArrayTool.transferValues(chunk_buffer, input_buffer, 0, position_in_input_buffer, chunk_buffer.length);
						position_in_input_buffer += chunk_buffer.length;
						
						if( !ElementChecker.checkAllElementsForZero(chunk_buffer) ) {
							
							chunk_data = ChunkUtility.getChunkData(chunk_buffer);
							chunk = new Chunk(this, chunk_data.getStartAddress(), 
									chunk_data.getChunkLength(), current_chunk_number);
							
							if( chunk_data.getFlakeIdentification() == FlakeManager.ROOT_IDENTIFICATION ) {
								addAvailableChunk(chunk);
							}
							else {
								flake_identificaiton = new Long(chunk_data.getFlakeIdentification());
								if( !declared_flakes_identification_list.contains(flake_identificaiton) ) {
									declared_flakes_identification_list.add(flake_identificaiton);
								}
								flake_modifier.addChunkToFlake(chunk_data.getFlakeIdentification(), chunk, 
										chunk_data.getIndexInFlake(), this);
							}
							
						}
						
						current_chunk_number++;
						
					}
					
					
					remaining_bytes_in_file = length_of_file - current_position;
					
				}
				
			}
			catch (IOException e) {
				throw new IOException("Can not read from the chunk-table-file \""
						+ chunk_manager_configuration.getChunkTableFilePath() + "\" at position: " + current_position
						+ " | chunk: " + current_chunk_number, e);
			}
			
			
		}
		
		flake_modifier.openFlakes(StreamFactory.getStream(declared_flakes_identification_list, StreamMode.Parallel)
				.mapToLong(identification -> identification.longValue()));
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param nopc_treshold {@link snowflake.core.manager.ChunkMergingManager#mergeChunks same as in mergeChunks}
	 */
	public void mergeAvailableChunks(int nopc_treshold) {
		if( !isOpen() ) {
			return;
		}
		else {
			chunk_merging_manager.mergeChunks(nopc_treshold);
		}		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Stream<IChunkInformation> streamAvailableChunks(StreamMode stream_mode) {
		synchronized( available_chunk_lock ) {
			return Stream.concat(
				available_chunk_list.getStream(stream_mode, StreamFilter::filterNull).<IChunkInformation>map(_o->_o),
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
	private Chunk trimToSizeUnsafe(Chunk chunk, long size) {
		if( chunk.isPartOfFlake() ) {
			throw new SecurityException("Do not ever trim a chunk unsafe when the chunk is part of a flake!");
		}
		Chunk[] split_chunks = splitChunk(chunk, size);
		addAvailableChunk(split_chunks[1]);
		return split_chunks[0];
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
		
		if( hasBeenOpened() ) {
			return;
		}

		closure_state = ClosureState.InOpening;
		index_manager.open();
		data_table.open();
		loadChunks();
		closure_state = ClosureState.Open;
		
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IClose#close()
	 */
	@Override public void close() throws IOException {
		
		if( !isOpen() ) {
			return;
		}

		closure_state = ClosureState.InClosure;
		index_manager.close();
		data_table.close();
		closure_state = ClosureState.Closed;
		
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkMemory#deleteChunk(snowflake.core.Chunk)
	 */
	@Override public void deleteChunk(Chunk chunk) {
		if( chunk == null || !chunk.isValid() ) {
			return;
		}
		else {
			data_table.addEntry(new BinaryDataWrapper<>(ChunkManager.NULL_CHUNK_DATA, chunk.getChunkTableIndex()));
			index_manager.addAvailableIndex(chunk.getChunkTableIndex());
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkMemory#saveChunk(snowflake.core.Chunk)
	 */
	@Override public void saveChunk(Flake owner_flake, Chunk chunk) {
		
		if( !ArgumentChecker.checkForNull(chunk, "chunk").isValid() ) {
			return;
		}
		
		data_table.addEntry(new BinaryDataWrapper<>(ChunkUtility.getChunkData(owner_flake, chunk), 
				chunk.getChunkTableIndex()));
		
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkManager#appendChunk(snowflake.core.flake.Flake, long, snowflake.api.ChunkAppendingMode)
	 */
	@Override public void appendChunk(Flake flake, long number_of_bytes, ChunkAppendingMode chunk_appending_mode) {
		
		switch( chunk_appending_mode ) {
		
			case SINGLE_CHUNK:
				flake.addChunk(getAvailableChunk(number_of_bytes));
				break;
			
			case MULTIPLE_CHUNKS:
				flake.addChunks(getAvailableChunks(number_of_bytes));
				break;
				
			default:
				throw new IllegalArgumentException("Can not support chunk_appending_mode \"" 
													+ chunk_appending_mode.toString() + "\"!");
				
		}
		
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkManager#recycleChunk(snowflake.core.Chunk)
	 */
	@Override public void recycleChunk(Chunk chunk) {
		if( chunk != null && chunk.isValid() ) {
			synchronized( available_chunk_lock ) {
				if( available_chunk_list.contains(chunk) || chunk_merging_manager.contains(chunk) ) {
						return;
				}
			}
			clear_chunk.clearChunk(chunk);
			chunk.resetPositionInFlake();
			chunk.save(null);
			addAvailableChunk(chunk);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkManager#mergeChunks(snowflake.core.Chunk[])
	 */
	@Override public Chunk mergeChunks(Chunk[] chunks) {
		
		ArgumentChecker.checkForNull(chunks, "chunks");
		
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
		
		
		Chunk merged_chunk = new Chunk(this, start_address, length, index_manager.getAvailableIndex());
		merged_chunk.setPositionInFlake(position_in_flake);
		merged_chunk.save(null);
		
		return merged_chunk;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkManager#mergeChunks(java.util.Collection)
	 */
	@Override public Chunk mergeChunks(Collection<Chunk> chunk_collection) {
		return mergeChunks(chunk_collection.toArray(new Chunk[0]));
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkManager#splitChunk(snowflake.core.Chunk, long)
	 */
	@Override public Chunk[] splitChunk(Chunk chunk, long position) {
		
		ArgumentChecker.checkForNull(chunk, "chunk");
		ArgumentChecker.checkForBoundaries(position, 1, chunk.getLength() - 1, "position");
		
		Chunk[] split_chunk = new Chunk[2];
		split_chunk[0] = new Chunk(this, chunk.getStartAddress(), position, index_manager.getAvailableIndex());
		split_chunk[1] = new Chunk(this, chunk.getStartAddress() + position, 
				chunk.getLength() - position, index_manager.getAvailableIndex());
		
		if( chunk.getPositionInFlake() >= 0 ) {
			split_chunk[0].setPositionInFlake( chunk.getPositionInFlake() );
			split_chunk[1].setPositionInFlake( chunk.getPositionInFlake() + split_chunk[0].getLength() );
		}
		
		chunk.delete();
		split_chunk[0].save(null);
		split_chunk[1].save(null);
		
		return split_chunk;
		
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkManager#trimToSize(snowflake.core.Chunk, long)
	 */
	@Override public Chunk trimToSize(Chunk chunk, long size) {
		Chunk[] split_chunks = splitChunk(chunk, size);
		recycleChunk(split_chunks[1]);
		return split_chunks[0];
	}
	
}
