package snowflake.core.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import j3l.util.ClosureState;
import j3l.util.RandomFactory;
import j3l.util.check.ArgumentChecker;
import j3l.util.close.IClose;
import j3l.util.stream.StreamFactory;
import j3l.util.stream.StreamMode;
import snowflake.api.flake.IFlake;
import snowflake.api.flake.IFlakeManager;
import snowflake.core.Chunk;
import snowflake.core.flake.Flake;
import snowflake.core.flake.FlakeDataManager;
import snowflake.core.flake.FlakeStreamManager;
import snowflake.core.storage.IRead;
import snowflake.core.storage.IWrite;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.12_0
 * @author Johannes B. Latzel
 */
public final class FlakeManager implements IFlakeManager, IFlakeModifier, IClose<IOException> {
	
	
	/**
	 * <p></p>
	 */
	public final static long ROOT_IDENTIFICATION = 0;
	
	
	/**
	 * <p></p>
	 */
	private final IWrite write;
	
	
	/**
	 * <p></p>
	 */
	private final IRead read;
	
	
	/**
	 * <p></p>
	 */
	private final Hashtable<Long, Flake> flake_table;
	
	
	/**
	 * <p></p>
	 */
	private ClosureState closure_state;
	
	
	/**
	 * <p></p>
	 */
	private final Random random;
	
	
	/**
	 * <p></p>
	 */
	private final Object flake_creation_lock;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FlakeManager(IRead read, IWrite write) {

		ArgumentChecker.checkForNull(read, "read");
		ArgumentChecker.checkForNull(write, "write");
		
		this.read = read;
		this.write = write;
		flake_table = new Hashtable<>();
		closure_state = ClosureState.None;
		flake_creation_lock = new Object();
		random = RandomFactory.createRandom();
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Stream<IFlake> streamFlakes(StreamMode stream_mode) {
		return StreamFactory.getStream(new ArrayList<>(flake_table.values()), stream_mode);
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
		flake_table.values().parallelStream().filter(flake -> flake.isValid()).forEach(flake -> flake.close());
		closure_state = ClosureState.Closed;
		
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlakeManager#createFlake(IChunkManager)
	 */
	@Override public IFlake createFlake(ChunkManager chunk_manager) {
		
		if( !isOpen() ) {
			throw new SecurityException("The FlakeManager is not open!");
		}

		
		long identification;
		Flake flake;
		
		synchronized( flake_creation_lock ) {
			
			do {
				identification = random.nextLong();
			}
			while( flakeExists(identification) || identification == FlakeManager.ROOT_IDENTIFICATION );
			
			flake = new Flake(identification);
			flake_table.put(new Long(identification), flake);
			
			flake.setFlakeDataManager(new FlakeDataManager(flake, chunk_manager));
			flake.setFlakeStreamManager(new FlakeStreamManager(read, write));
			flake.open();
			
		}
		
		return flake;
		
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlakeManager#getFlake(long)
	 */
	@Override public IFlake getFlake(long identification) {
		return flake_table.get(new Long(identification));
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlakeManager#flakeExists(long)
	 */
	@Override public boolean flakeExists(long identification) {
		if( identification == FlakeManager.ROOT_IDENTIFICATION ) {
			return true;
		}
		else {
			return flake_table.get(new Long(identification)) != null;
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.IFlakeManager#declareFlake(long, snowflake.core.manager.ChunkManager)
	 */
	@Override public IFlake declareFlake(long identification, ChunkManager chunk_manager) {
		
		if( identification == FlakeManager.ROOT_IDENTIFICATION ) {
			throw new IllegalArgumentException("The identification must not be equal to the ROOT_IDENTIFICATION "
					+  "\"" + FlakeManager.ROOT_IDENTIFICATION + "\"!");
		}
		
		if( flakeExists(identification) ) {
			return getFlake(identification);
		}
		
		
		Flake flake;
		
		synchronized( flake_creation_lock ) {
			flake = new Flake(identification);
			
			flake.setFlakeDataManager(new FlakeDataManager(flake, chunk_manager));
			flake.setFlakeStreamManager(new FlakeStreamManager(read, write));

			flake_table.put(new Long(identification), flake);
		}
		
		return flake;
		
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.manager.IFlakeModifier#addChunkToFlake(long, snowflake.core.Chunk, int)
	 */
	@Override public void addChunkToFlake(long identification, Chunk chunk, int index, ChunkManager chunk_manager) {
		if( !flakeExists(identification) ) {
			declareFlake(identification, chunk_manager);
		}
		// will eventually be closed by this instance
		@SuppressWarnings("resource") Flake flake = flake_table.get(new Long(identification));
		ArgumentChecker.checkForNull(flake, "flake").insertChunk(chunk, index);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.manager.IFlakeModifier#openFlakes(java.util.stream.Stream)
	 */
	@Override public void openFlakes(LongStream flake_identification_stream) {
		ArgumentChecker.checkForNull(flake_identification_stream, "flake_identification_stream")
		.filter(identification -> flakeExists(identification)).forEach(identification -> {
			// will eventually be closed by this instance
			@SuppressWarnings("resource") Flake flake = flake_table.get(new Long(identification));
			if( flake != null ) {
				flake.open();
			}
		});
	}
	
	
}
