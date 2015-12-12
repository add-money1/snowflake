package snowflake.core.flake;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import j3l.util.ClosureState;
import j3l.util.check.ArgumentChecker;
import j3l.util.check.ClosureChecker;
import j3l.util.check.ValidationChecker;
import j3l.util.close.IClose;
import snowflake.api.chunk.IChunkInformation;
import snowflake.api.flake.IFlake;
import snowflake.api.flake.Lock;
import snowflake.core.Chunk;
import snowflake.core.stream.FlakeInputStream;
import snowflake.core.stream.FlakeOutputStream;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.07_0
 * @author Johannes B. Latzel
 */
public final class Flake implements IClose<IOException>, IFlake {
	
	
	/**
	 * <p></p>
	 */
	private FlakeDataManager flake_data_manager;
	
	
	/**
	 * <p></p>
	 */
	private final FlakeLockManager flake_lock_manager;
	
	
	/**
	 * <p></p>
	 */
	private FlakeStreamManager flake_stream_manager;
	
	
	/**
	 * <p></p>
	 */
	private final long identification;
	
	
	/**
	 * <p></p>
	 */
	private ClosureState closure_state;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_damaged;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_deleted;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_chunk_merging;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Flake(long identification) {
		
		this.identification = identification;
		flake_lock_manager = new FlakeLockManager();
		closure_state = ClosureState.None;
		is_damaged = false;
		is_deleted = false;
		is_chunk_merging = false;
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setFlakeStreamManager(FlakeStreamManager flake_stream_manager) {
		
		if( hasBeenOpened() ) {
			throw new SecurityException("Can not change the flake_stream_manager after the flake has been opened!");
		}
		
		ArgumentChecker.checkForNull(flake_stream_manager,"flake_stream_manager");
		this.flake_stream_manager = flake_stream_manager;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setFlakeDataManager(FlakeDataManager flake_data_manager) {

		if( hasBeenOpened() ) {
			throw new SecurityException("Can not change the flake_stream_manager after the flake has been opened!");
		}

		ArgumentChecker.checkForNull(flake_data_manager,"flake_data_manager");
		this.flake_data_manager = flake_data_manager;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private boolean checkForConsistency() {
		return (flake_data_manager != null) && (flake_stream_manager != null) && flake_data_manager.isConsistent();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public int getIndexOfChunk(Chunk chunk) {
		ValidationChecker.checkForValidation(this);
		return flake_data_manager.getIndexOfChunk(chunk);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void addChunks(Chunk[] chunks) {
		ValidationChecker.checkForValidation(this);
		flake_data_manager.addChunks(chunks);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void addChunks(Collection<Chunk> chunk_collection) {
		ValidationChecker.checkForValidation(this);
		flake_data_manager.addChunks(chunk_collection);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void addChunk(Chunk chunk) {
		ValidationChecker.checkForValidation(this);
		flake_data_manager.addChunk(chunk);
	}


	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void insertChunk(Chunk chunk, int index) {
		if( flake_data_manager != null && !hasBeenOpened() ) {
			flake_data_manager.insertChunk(chunk, index);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IStateClosure#getClosureState()
	 */
	@Override public ClosureState getClosureState() {
		return closure_state;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#delete()
	 */
	@Override public synchronized boolean delete() {
		
		if( !isValid() ) {
			return false;
		}
		
		Lock lock;
		
		if( !hasBeenOpened() || isLocked() ) {
			return false;
		}
		else {
			lock = lock(this);
		}
		
		
		if( isOpen() ) {
			close();
		}
		
		flake_data_manager.setLength(0);
		is_deleted = true;
		lock.releaseLock();
		
		return true;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#isValid()
	 */
	@Override public boolean isValid() {
		return !isDamaged() && !isDeleted() && isOpen();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getLength()
	 */
	@Override public long getLength() {
		ValidationChecker.checkForValidation(this);
		return flake_data_manager.getLength();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#setLength(long)
	 */
	@Override public void setLength(long new_length) {
		ValidationChecker.checkForValidation(this);
		flake_data_manager.setLength(new_length);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getNumberOfChunks()
	 */
	@Override public int getNumberOfChunks() {
		ValidationChecker.checkForValidation(this);
		return flake_data_manager.getNumberOfChunks();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#mergeChunks()
	 */
	@Override public synchronized void mergeChunks() {
		if( needsChunkMerging() ) {
			is_chunk_merging = true;
			flake_data_manager.mergeChunks();
			is_chunk_merging = false;
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#needsChunkMerging()
	 */
	@Override public boolean needsChunkMerging() {
		return isValid() && flake_data_manager.needsChunkMerging();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#isDamaged()
	 */
	@Override public boolean isDamaged() {
		return is_damaged;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getChunks()
	 */
	@Override public IChunkInformation[] getChunks() {
		ValidationChecker.checkForValidation(this);
		return flake_data_manager.getChunks();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getChunkAtIndex(int)
	 */
	@Override public IChunkInformation getChunkAtIndex(int index) {
		ValidationChecker.checkForValidation(this);
		return flake_data_manager.getChunkAtIndex(index);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getChunkAtPositionInFlake(long)
	 */
	@Override public IChunkInformation getChunkAtPositionInFlake(long position_in_flake) {
		ValidationChecker.checkForValidation(this);
		return flake_data_manager.getChunkAtPosition(position_in_flake);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getFlakeInputStream()
	 */
	@Override public FlakeInputStream getFlakeInputStream() throws IOException {
		if( !isStreamable() ) {
			throw new SecurityException("The flake can not be streamed!");
		}
		return flake_stream_manager.getFlakeInputStream(this);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getFlakeOutputStream()
	 */
	@Override public FlakeOutputStream getFlakeOutputStream() throws IOException {
		
		if( !isStreamable() ) {
			throw new SecurityException("The flake can not be streamed!");
		}
		
		if( isWriting() ) {
			throw new SecurityException("Can not create a FlakeOutputStream while another is still writing to this flake!");
		}
		
		return flake_stream_manager.getFlakeOutputStream(this);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#isLocked()
	 */
	@Override public boolean isLocked() {
		return isValid() && flake_lock_manager.isLocked();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#lock(java.lang.Object)
	 */
	@Override public Lock lock(Object owner) {
		ClosureChecker.checkForOpen(this, toString());
		return flake_lock_manager.lock(owner);
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IClose#open()
	 */
	@Override public void open() {
		
		if( hasBeenOpened() ) {
			return;
		}
		
		closure_state = ClosureState.InOpening;
		
		if( flake_data_manager != null ) {
			flake_data_manager.orderChunks();
		}
		
		if( !checkForConsistency() ) {
			is_damaged = true;
		}
		
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
		
		if( isValid() ) {
			LinkedList<IOException> exception_list = flake_stream_manager.closeAllStreams();
			for( IOException e : exception_list ) {
				e.printStackTrace();
			}
			System.out.println("Flake.close() -> was mit den ganzen ioexceptions anfangen?");
		}
		
		closure_state = ClosureState.Closed;
		
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		return "Flake[" + getIdentification() + "]";
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override public int hashCode() {
		return Long.hashCode(identification);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override public boolean equals(Object object) {
		
		if( object != null && object instanceof IFlake ) {
			
			IFlake flake = (IFlake)object;
			
			if( flake.hashCode() == hashCode() && flake.getIdentification() == getIdentification() ) {
				return true;
			}
			
		}
		
		return false;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getIdentification()
	 */
	@Override public long getIdentification() {
		return identification;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#isDeleted()
	 */
	@Override public boolean isDeleted() {
		return is_deleted;
	}


	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#isWriting()
	 */
	@Override public boolean isWriting() {
		return isValid() && flake_stream_manager.isWriting();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#isChunkMerging()
	 */
	@Override public boolean isChunkMerging() {
		return is_chunk_merging;
	}
	
}
