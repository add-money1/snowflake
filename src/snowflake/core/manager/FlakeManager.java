package snowflake.core.manager;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Random;

import j3l.util.Checker;
import j3l.util.ClosureState;
import j3l.util.IClose;
import j3l.util.RandomFactory;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.api.IFlake;
import snowflake.api.StorageException;
import snowflake.core.Chunk;
import snowflake.core.Flake;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.11_0
 * @author Johannes B. Latzel
 */
public final class FlakeManager implements IFlakeManager, IClose<StorageException> {
	
	
	/**
	 * <p></p>
	 */
	public final static long ROOT_IDENTIFICATION = 0;
	
	
	/**
	 * <p></p>
	 */
	private final IChannelManager channel_manager;
	
	
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
	public FlakeManager(IChannelManager channel_manager) {
		if( StaticMode.TESTING_MODE ) {
			this.channel_manager = Checker.checkForNull(
				channel_manager, GlobalString.ChannelManager.toString()
			);
		}
		else {
			this.channel_manager = channel_manager;
		}
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
	public ArrayList<IFlake> getFlakes() {
		return new ArrayList<>(flake_table.values());
	}
	
	
	/**
	 * <p></p>
	 */
	public IFlake getSpecialFlake(SpecialFlakeIdentification special_flake_identification, ChunkManager chunk_manager) {
		long identification = special_flake_identification.getIdentification();
		if( !flakeExists(identification) ) {
			synchronized( flake_creation_lock ) {
				Flake flake = new Flake(identification);
				flake.initialize(channel_manager, chunk_manager, null);
				flake.open();
				flake_table.put(new Long(identification), flake);
				return flake;
			}
		}
		return getFlake(identification);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public IFlake declareFlake(long identification, ChunkManager chunk_manager) {
		return declareFlake(identification, chunk_manager, null);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public IFlake declareFlake(long identification, ChunkManager chunk_manager, ArrayList<Chunk> initial_chunk_list) {
		if( identification == FlakeManager.ROOT_IDENTIFICATION ) {
			throw new IllegalArgumentException("The identification must not be equal to the ROOT_IDENTIFICATION "
					+  "\"" + FlakeManager.ROOT_IDENTIFICATION + "\"!");
		}
		if( flakeExists(identification) ) {
			return getFlake(identification);
		}
		synchronized( flake_creation_lock ) {
			Flake flake = new Flake(identification);
			flake.initialize(channel_manager, chunk_manager, initial_chunk_list);			
			flake_table.put(new Long(identification), flake);
			return flake;
		}
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
	@Override public void open() {
		if( hasBeenOpened() ) {
			return;
		}
		closure_state = ClosureState.InOpening;
		flake_table.values().forEach(Flake::open);
		closure_state = ClosureState.Open;
	}


	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IClose#close()
	 */
	@Override public void close() {
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
			while(
				flakeExists(identification)
				|| identification == FlakeManager.ROOT_IDENTIFICATION
				|| identification == SpecialFlakeIdentification.FlakeTable.getIdentification()
				| identification == SpecialFlakeIdentification.DirectoryTable.getIdentification()
				|| identification == SpecialFlakeIdentification.DeduplicationTable.getIdentification()
			);
			flake = new Flake(identification);
			flake_table.put(new Long(identification), flake);
			flake.initialize(channel_manager, chunk_manager, null);
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
		return flake_table.get(new Long(identification)) != null;
	}
	
}
