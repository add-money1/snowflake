package snowflake.core.storage;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Stream;

import j3l.util.ArrayTool;
import j3l.util.check.ArgumentChecker;
import j3l.util.check.ElementChecker;
import j3l.util.close.ClosureState;
import j3l.util.close.IClose;
import j3l.util.stream.StreamFilter;
import j3l.util.stream.StreamMode;
import snowflake.GlobalString;
import snowflake.api.IFlake;
import snowflake.api.IStorageInformation;
import snowflake.api.StorageException;
import snowflake.core.Chunk;
import snowflake.core.ChunkData;
import snowflake.core.ChunkUtility;
import snowflake.core.IChunk;
import snowflake.core.manager.ChannelManager;
import snowflake.core.manager.ChunkManager;
import snowflake.core.manager.FlakeManager;


/**
 * <p>storage</p>
 * 
 * @since JDK 1.8
 * @version 2016.06.17_0
 * @author Johannes B. Latzel
 */
public final class Storage implements IStorageInformation, IAllocateSpace, 
										IClearChunk, IClose<IOException> {
	
	
		
	/**
	 * <p></p>
	 */
	private final ChunkManager chunk_manager;
	
	
	/**
	 * <p></p>
	 */
	private final FlakeManager flake_manager;
	
	
	/**
	 * <p></p>
	 */
	private final ChannelManager channel_manager;
	
	
	/**
	 * <p></p>
	 */
	private final StorageConfiguration storage_configuration;
	
	
	/**
	 * <p></p>
	 */
	private ClosureState closure_state;
	
	
	/**
	 * <p></p>
	 */
	private final RandomAccessFile data_file;
	
	
	/**
	 * <p></p>
	 */
	private final byte[] clear_array;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	public Storage(StorageConfiguration storage_configuration) throws IOException {
		this.storage_configuration = ArgumentChecker.checkForNull(
			storage_configuration, GlobalString.StorageConfiguration.toString()
		);
		channel_manager 		= 	new ChannelManager(storage_configuration);
		flake_manager 			= 	new FlakeManager(channel_manager);
		chunk_manager 			= 	new ChunkManager(this, this, storage_configuration, this);
		data_file 				= 	new RandomAccessFile(storage_configuration.getDataFilePath(), "rw");
		clear_array 			= 	new byte[ storage_configuration.getClearArraySize() ];
		closure_state 			= 	ClosureState.None;
		open();
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
		
		
		long current_chunk_number = 0;
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
		
		ArrayList<Chunk> recycle_chunk_list = new ArrayList<>(1000);
		ArrayList<Chunk> available_chunk_list = new ArrayList<>(1000);
		ArrayList<Long> available_index_list = new ArrayList<>(1000);
		HashMap<Long, ArrayList<Chunk>> flake_list = new HashMap<>();
		ArrayList<Chunk> current_flake_chunk_list;
		Long current_flake_identification;
		
		File chunk_table_file = new File(storage_configuration.getChunkTableFilePath());
		
		
		length_of_file = chunk_table_file.length();
		
		if( length_of_file == 0 ) {
			return;
		}
		else if( length_of_file % ChunkUtility.BINARY_CHUNK_SIZE != 0 ) {
			throw new SecurityException("The file \"" + storage_configuration.getChunkTableFilePath()
			+ "\" has been damaged!");
		}
		
		
		try (BufferedInputStream input_stream = new BufferedInputStream(new FileInputStream(chunk_table_file))) {
			
			
			// important, because (int)(length_of_file / ChunkUtility.BINARY_CHUNK_SIZE) could overflow!
			if( length_of_file > (ChunkUtility.BINARY_CHUNK_SIZE * (long)(Integer.MAX_VALUE)) ) {
				throw new IOException("The size of the file \"" + storage_configuration.getChunkTableFilePath() 
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
						chunk = new Chunk(chunk_manager, chunk_data.getStartAddress(), 
								chunk_data.getChunkLength(), current_chunk_number);
						ChunkUtility.configureChunk(chunk, chunk_data.getFlagVector());
						
						if( chunk_data.getFlakeIdentification() == FlakeManager.ROOT_IDENTIFICATION ) {
							
							if( chunk.needsToBeCleared() ) {
								recycle_chunk_list.add(chunk);
							}
							else {
								available_chunk_list.add(chunk);
							}
							
						}
						else {
							int current_index_in_flake = chunk_data.getIndexInFlake();
							current_flake_identification = new Long(chunk_data.getFlakeIdentification());
							if( !flake_list.containsKey(current_flake_identification) ) {
								flake_list.put(current_flake_identification, new ArrayList<>(1));
							}
							current_flake_chunk_list = flake_list.get(current_flake_identification);
							if( current_flake_chunk_list.size() < current_index_in_flake ) {
								do {
									current_flake_chunk_list.add(null);
								}
								while( current_flake_chunk_list.size() < chunk_data.getIndexInFlake() );
							}
							current_flake_chunk_list.add(current_index_in_flake, chunk);
							// flake_modifier.addChunkToFlake(current_flake_identification.longValue(), chunk, 
							//		chunk_data.getIndexInFlake(), this);
						}
						
					}
					else {
						// index is available
						available_index_list.add(new Long(current_chunk_number));
					}
					
					current_chunk_number++;
					
				}
				
				
				remaining_bytes_in_file = length_of_file - current_position;
				
			}
			
		}
		catch (IOException e) {
			throw new IOException("Can not read from the chunk-table-file \""
					+ storage_configuration.getChunkTableFilePath() + "\" at position: " + current_position
					+ " | chunk: " + current_chunk_number, e);
		}
		
		chunk_manager.addAvailableChunks(available_chunk_list);
		chunk_manager.recycleChunks(recycle_chunk_list);
		chunk_manager.setInitialIndices(available_index_list);
		
		for( Long identification : flake_list.keySet() ) {
			current_flake_chunk_list = flake_list.get(identification);
			for(int a=current_flake_chunk_list.size()-1;a>=0;a--) {
				if( current_flake_chunk_list.get(a) == null ) {
					// should this happen? like, ever??
					current_flake_chunk_list.remove(a);
				}
			}
			flake_manager.declareFlake(identification.longValue(), chunk_manager, current_flake_chunk_list);
		}
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Stream<IFlake> getFlakes(StreamMode stream_mode) {
		return flake_manager.streamFlakes(stream_mode);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Stream<IChunk> getAvailableChunks(StreamMode stream_mode) {
		return chunk_manager.streamAvailableChunks(stream_mode);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public IFlake createFlake() {
		return flake_manager.createFlake(chunk_manager);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public IFlake getFlake(long indentification) {
		return flake_manager.getFlake(indentification);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean flakeExists(long identification) {
		return flake_manager.flakeExists(identification);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public IFlake getFileTableFlake() {
		return flake_manager.getFileTableFlake(chunk_manager);
	}


	/**
	 * @return
	 */
	public IFlake getDirectoryTableFlake() {
		return flake_manager.getDirectoryTableFlake(chunk_manager);
	}
	
				
	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IClose#open()
	 */
	@Override public void open() throws IOException {
		
		if( hasBeenOpened() ) {
			return;
		}
		
		closure_state = ClosureState.InOpening;
		
		loadChunks();
		
		flake_manager.open();
		chunk_manager.open();
				
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
		
		flake_manager.close();
		chunk_manager.close();
		storage_configuration.saveConfiguration();
		
		closure_state = ClosureState.Closed;
		
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IStateClosure#getClosureState()
	 */
	@Override public ClosureState getClosureState() {
		return closure_state;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.storage.IClearChunk#clearChunk(snowflake.core.data.Chunk, long, length)
	 */
	@Override public void clearChunk(Chunk chunk, long offset, long length) throws StorageException {
		
		if( chunk == null || !chunk.isValid() ) {
			return;
		}
		
		if( chunk.getLength() - offset < length ) {
			throw new IndexOutOfBoundsException("The length must not be greater than chunk.getLength() - offset!");
		}
		
		long remaining_bytes = length;
		int clear_array_size = clear_array.length;
		synchronized( data_file ) {
			try {
				data_file.seek(chunk.getStartAddress() + offset);
				if( remaining_bytes > clear_array.length ) {
					do {
						data_file.write(clear_array, 0, clear_array_size);
						remaining_bytes -= clear_array_size;
					}
					while( remaining_bytes >= clear_array_size );
				}
				if( remaining_bytes > 0 ) {
					// cast is okay, because remaining_bytes is smaller than or equal to clear_array.length (which is int)
					data_file.write(clear_array, 0, (int)remaining_bytes);
					remaining_bytes = 0;
				}
			}
			catch( IOException e ) {
				throw new StorageException("Failed to clear the chunk \"" + chunk.toString() + "\"!", e);
			}
		}
		
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IAllocateSpace#allocateSpace(long)
	 */
	@Override public ChunkData allocateSpace(long number_of_bytes) {
		ArgumentChecker.checkForBoundaries(number_of_bytes, 1, Long.MAX_VALUE, GlobalString.NumberOfBytes.toString());
		ChunkData chunk_data;
		long new_length;
		long current_length;
		synchronized( data_file ) {
			long maximum_storage_size = storage_configuration.getMaximumStorageSize();
			current_length = getAllocatedSpace();
			if( current_length > maximum_storage_size ) {
				throw new StorageException("The storage is larger than the " 
						+ StorageConfigurationElement.MaximumStorageSize.getName());
			}
			new_length = Math.min(current_length + number_of_bytes, maximum_storage_size);
			if( new_length == current_length ) {
				throw new StorageException("There is not enough capacity available to allocate a new chunk!");
			}
			chunk_data = new ChunkData(current_length, new_length - current_length, FlakeManager.ROOT_IDENTIFICATION, 0, (byte)0);
			try {
				data_file.setLength(new_length);
			} catch (IOException e) {
				throw new StorageException("Failed to allocate \"" + number_of_bytes  + "\" bytes!", e);
			}
		}
		return chunk_data;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getNumberOfFlakes()
	 */
	@Override public int getNumberOfFlakes() {
		long number_of_flakes = flake_manager.streamFlakes(StreamMode.Parallel).count();
		if( number_of_flakes > Integer.MAX_VALUE ) {
			return Integer.MAX_VALUE;
		}
		else {
			// cast okay, because number_of_flakes is smaller than or equal to Integer.MAX_VALUE
			return (int)number_of_flakes;
		}
	}

	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getNumberOfDamagedFlakes()
	 */
	@Override public int getNumberOfDamagedFlakes() {
		long number_of_damaged_flakes = flake_manager.streamFlakes(StreamMode.Parallel).filter(flake -> flake.isDamaged()).count();
		if( number_of_damaged_flakes > Integer.MAX_VALUE ) {
			return Integer.MAX_VALUE;
		}
		else {
			return (int)number_of_damaged_flakes;
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getAverageFlakeSize()
	 */
	@Override public double getAverageFlakeSize() {
		long number_of_flakes = getNumberOfFlakes();
		if( number_of_flakes == 0 ) {
			return 0d;
		}
		return (double)number_of_flakes / getUsedSpace();	
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getAverageNumberOfChunksPerFlake()
	 */
	@Override public double getAverageNumberOfChunksPerFlake() {
		long number_of_flakes = getNumberOfFlakes();
		if( number_of_flakes == 0 ) {
			return 0d;
		}
		return (double)getNumberOfUsedChunks() / number_of_flakes;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getNumberOfUsedChunks()
	 */
	@Override public long getNumberOfUsedChunks() {
		return flake_manager.streamFlakes(StreamMode.Parallel).mapToLong(flake -> flake.getNumberOfChunks()).sum();
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getNumberOfFreeChunks()
	 */
	@Override public long getNumberOfFreeChunks() {
		return chunk_manager.streamAvailableChunks(StreamMode.Parallel).count();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getAverageChunkSize()
	 */
	@Override public double getAverageChunkSize() {
		long number_of_chunks = getNumberOfChunks();
		if( number_of_chunks == 0 ) {
			return 0d;
		}
		return (double)getAllocatedSpace() / number_of_chunks;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getFreeSpace()
	 */
	@Override public long getFreeSpace() {
		return chunk_manager.streamAvailableChunks(StreamMode.Parallel).mapToLong(chunk -> chunk.getLength()).sum();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getUsedSpace()
	 */
	@Override public long getUsedSpace() {
		return flake_manager.streamFlakes(StreamMode.Parallel).filter(StreamFilter::filterInvalid)
			.mapToLong(flake -> flake.getLength()).sum();
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getAllocatedSpace()
	 */
	@Override public long getAllocatedSpace() {
		synchronized( data_file ) {
			try {
				return data_file.length();
			} catch (IOException e) {
				throw new StorageException("Failed to retrieve the number of allocated bytes!", e);
			}
		}
	}
	
}
