package snowflake.core.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.EventListener;
import java.util.stream.Stream;

import j3l.util.ClosureState;
import j3l.util.check.ArgumentChecker;
import j3l.util.check.ClosureChecker;
import j3l.util.close.IClose;
import j3l.util.stream.StreamMode;
import j3l.util.transform.TransformValue;
import snowflake.api.chunk.IChunkInformation;
import snowflake.api.flake.DataPointer;
import snowflake.api.flake.IFlake;
import snowflake.api.storage.IListenerAdapter;
import snowflake.api.storage.IManagerAdapter;
import snowflake.api.storage.IStorageInformation;
import snowflake.api.storage.StorageException;
import snowflake.core.Chunk;
import snowflake.core.data.ChunkData;
import snowflake.core.manager.ChunkManager;
import snowflake.core.manager.FlakeManager;


/**
 * <p>storage</p>
 * 
 * @since JDK 1.8
 * @version 2015.12.12_0
 * @author Johannes B. Latzel
 */
public final class Storage implements IListenerAdapter, IStorageInformation, IManagerAdapter, 
												IRead, IWrite, IAllocateSpace, IClearChunk, IClose<IOException> {
	
	
		
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
	private final StorageConfiguration storage_configuration;
	
	
	/**
	 * <p></p>
	 */
	private ClosureState closure_state;
	
	
	/**
	 * <p></p>
	 */
	private final RandomAccessFile data_input_file;
	
	
	/**
	 * <p></p>
	 */
	private final RandomAccessFile data_output_file;
	
	
	/**
	 * <p></p>
	 */
	private final Object write_lock;
	
	
	/**
	 * <p></p>
	 */
	private final Object read_lock;
	
	
	/**
	 * <p></p>
	 */
	private final Object event_lock;
	
	
	/**
	 * <p></p>
	 */
	private final ArrayList<EventListener> event_lister_list;
	
	
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
		
		ArgumentChecker.checkForNull(storage_configuration, "storage_configuration");
		this.storage_configuration = storage_configuration;
		
		flake_manager = new FlakeManager(this, this);
		chunk_manager = new ChunkManager(this, this, storage_configuration, this, flake_manager);

		data_input_file = new RandomAccessFile(storage_configuration.getDataFilePath(), "r");
		data_output_file = new RandomAccessFile(storage_configuration.getDataFilePath(), "rw");
		
		clear_array = new byte[ storage_configuration.getClearArraySize() ];
		
		event_lister_list = new ArrayList<>();
		read_lock = new Object();
		write_lock = new Object();
		event_lock = new Object();
		closure_state = ClosureState.None;
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void checkForOpen() {
		ClosureChecker.checkForOpen(this, "Storage");
		ClosureChecker.checkForOpen(flake_manager, "flake_manager");
		ClosureChecker.checkForOpen(chunk_manager, "chunk_manager");
	}
	
				
	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IClose#open()
	 */
	@Override public void open() throws IOException {
		
		if( hasBeenOpened() ) {
			return;
		}
		
		closure_state = ClosureState.InOpening;
		
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
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.storage.IManagerAdapter#declareFlake(long)
	 */
	@Override public IFlake declareFlake(long identification) {
		checkForOpen();
		return flake_manager.declareFlake(identification, chunk_manager);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.storage.IManagerAdapter#createFlake()
	 */
	@Override public IFlake createFlake() {
		checkForOpen();
		return flake_manager.createFlake(chunk_manager);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.storage.IManagerAdapter#getFlake(long)
	 */
	@Override public IFlake getFlake(long indentification) {
		checkForOpen();
		return flake_manager.getFlake(indentification);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlakeManager#flakeExists(long)
	 */
	@Override public boolean flakeExists(long identification) {
		checkForOpen();
		return flake_manager.flakeExists(identification);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IWrite#write(snowflake.api.DataPointer, byte)
	 */
	@Override public void write(DataPointer data_pointer, byte b) throws IOException {
		synchronized( write_lock ) {
			data_output_file.seek(data_pointer.getPositionInStorage());
			data_output_file.write(b);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IWrite#write(snowflake.api.DataPointer, byte[], int, int)
	 */
	@Override public void write(DataPointer data_pointer, byte[] buffer, int offset, int length) throws IOException {
		synchronized( write_lock ) {
			data_output_file.seek(data_pointer.getPositionInStorage());
			data_output_file.write(buffer, offset, length);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IRead#read(snowflake.api.DataPointer)
	 */
	@Override public byte read(DataPointer data_pointer) throws IOException {
		synchronized( read_lock ) {
			if( data_pointer.isEOF() ) {
				throw new IOException("Can not read from a eof-stated stream!");
			}
			data_input_file.seek(data_pointer.getPositionInStorage());
			return (byte)data_input_file.read();
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IRead#read(snowflake.api.DataPointer, byte[], int, int)
	 */
	@Override public int read(DataPointer data_pointer, byte[] buffer, int offset, int length) throws IOException {
		long remaining_bytes_in_flake;
		synchronized( read_lock ) {
			if( data_pointer.isEOF() ) {
				return 0;
			}
			data_input_file.seek(data_pointer.getPositionInStorage());
			remaining_bytes_in_flake =  data_pointer.getRemainingBytes();
			if( remaining_bytes_in_flake >= length ) {
				return data_input_file.read(buffer, offset, length);
			}
			else {
				return data_input_file.read(buffer, offset, (int)remaining_bytes_in_flake);
			}
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getFreeSpace()
	 */
	@Override public BigInteger getFreeSpace() {
		checkForOpen();
		return chunk_manager.getFreeSpace();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getUsedSpace()
	 */
	@Override public BigInteger getUsedSpace() {
		checkForOpen();
		ClosureChecker.checkForOpen(this, "FlakeManager");
		BigInteger used_space = new BigInteger(new byte[] {0});
		flake_manager.streamFlakes(StreamMode.Parallel).filter(flake -> flake != null && flake.isValid()).forEach(flake -> {
			used_space.add(
				new BigInteger(
					TransformValue.toByteArray(flake.getLength())
				)
			);
		});
		return used_space;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getAllocatedSpace()
	 */
	@Override public long getAllocatedSpace() {
		synchronized( read_lock ) {
			try {
				return data_input_file.length();
			} catch (IOException e) {
				throw new StorageException("Failed to retrieve the number of allocated bytes!", e);
			}
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IListenerAdapter#addListener(java.util.EventListener)
	 */
	@Override public void addListener(EventListener event_listener) {
		synchronized( event_lock ) {
			event_lister_list.add(event_listener);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IListenerAdapter#removeListener(java.util.EventListener)
	 */
	@Override public void removeListener(EventListener event_listener) {
		synchronized( event_lock ) {
			event_lister_list.remove(event_listener);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IClearChunk#clearChunk(snowflake.core.Chunk)
	 */
	@Override public void clearChunk(Chunk chunk) {
		ArgumentChecker.checkForNull(chunk, "chunk");
		long remaining_bytes = chunk.getLength();
		int clear_array_size = clear_array.length;
		synchronized( write_lock ) {
			try {
				data_output_file.seek(chunk.getStartAddress());
				if( remaining_bytes > clear_array.length ) {
					do {
						data_output_file.write(clear_array, 0, clear_array_size);
						remaining_bytes -= clear_array_size;
					}
					while( remaining_bytes > clear_array_size );
				}
				if( remaining_bytes > 0 ) {
					// cast is okay, because remaining_bytes is smaller than clear_array.length, which is int
					data_output_file.write(clear_array, 0, (int)remaining_bytes);
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
		ArgumentChecker.checkForBoundaries(number_of_bytes, 1, Long.MAX_VALUE, "number_of_bytes");
		ChunkData chunk_data;
		long new_length;
		synchronized( write_lock ) {
			new_length = getAllocatedSpace() + number_of_bytes;
			chunk_data = new ChunkData(getAllocatedSpace(), number_of_bytes, FlakeManager.ROOT_IDENTIFICATION, 0);
			try {
				data_output_file.setLength(new_length);
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
		ClosureChecker.checkForOpen(this, "FlakeManager");
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
		ClosureChecker.checkForOpen(this, "FlakeManager");
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
		BigDecimal used_space = new BigDecimal(getUsedSpace());
		return used_space.divide(new BigDecimal(
				new BigInteger(TransformValue.toByteArray(getNumberOfFlakes())))).doubleValue();		
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getAverageNumberOfChunksPerFlake()
	 */
	@Override public double getAverageNumberOfChunksPerFlake() {
		return getNumberOfChunks() * 1d / getNumberOfFlakes();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getNumberOfChunks()
	 */
	@Override public long getNumberOfChunks() {
		checkForOpen();
		return flake_manager.streamFlakes(StreamMode.Parallel).mapToLong(flake -> flake.getNumberOfChunks())
				.reduce((x,y) -> x + y).orElse(0) + chunk_manager.streamAvailableChunks(StreamMode.Parallel).count();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IStorageInformation#getAverageChunkSize()
	 */
	@Override public double getAverageChunkSize() {
		checkForOpen();
		return getAllocatedSpace() * 1.0d / getNumberOfChunks();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IManagerAdapter#mergeAvailableChunks(int)
	 */
	@Override public void mergeAvailableChunks(int number_of_attempts) {
		checkForOpen();
		chunk_manager.mergeAvailableChunks(number_of_attempts);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IManagerAdapter#getFlakes(j3l.util.interfaces.StreamMode)
	 */
	@Override public Stream<IFlake> getFlakes(StreamMode stream_mode) {
		checkForOpen();
		return flake_manager.streamFlakes(stream_mode);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.storage.IManagerAdapter#getAvailableChunks(j3l.util.interfaces.StreamMode)
	 */
	@Override public Stream<IChunkInformation> getAvailableChunks(StreamMode stream_mode) {
		checkForOpen();
		return chunk_manager.streamAvailableChunks(stream_mode);
	}
	
}
