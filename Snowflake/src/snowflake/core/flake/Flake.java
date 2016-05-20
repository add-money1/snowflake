 package snowflake.core.flake;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import j3l.util.check.ArgumentChecker;
import j3l.util.close.ClosureState;
import j3l.util.close.IClose;
import snowflake.api.FlakeInputStream;
import snowflake.api.FlakeOutputStream;
import snowflake.api.IFlake;
import snowflake.core.Chunk;
import snowflake.core.GlobalString;
import snowflake.core.IChunk;
import snowflake.core.manager.IChannelManager;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.05.03_0
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
	private IChannelManager channel_manager;
	
	
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
	 *
	 * @param
	 * @return
	 */
	public Flake(long identification) {
		this.identification = identification;
		closure_state = ClosureState.None;
		is_damaged = false;
		is_deleted = false;
		flake_data_manager = null;
		channel_manager = null;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setChannelManager(IChannelManager channel_manager) {
		if( hasBeenOpened() ) {
			throw new SecurityException("Can not change the flake_stream_manager after the flake has been opened!");
		}
		this.channel_manager = ArgumentChecker.checkForNull(
				channel_manager, GlobalString.ChannelManager.toString()
		);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setFlakeDataManager(FlakeDataManager flake_data_manager, ArrayList<Chunk> initial_chunk_list) {
		if( hasBeenOpened() ) {
			throw new SecurityException("Can not change the flake_stream_manager after the flake has been opened!");
		}
		this.flake_data_manager = 
			ArgumentChecker.checkForNull(flake_data_manager, GlobalString.FlakeDataManager.toString()
		);
		flake_data_manager.setInitialChunks(initial_chunk_list);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private boolean checkForConsistency() {
		return (flake_data_manager != null) && (channel_manager != null) && flake_data_manager.isConsistent();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public int getIndexOfChunk(Chunk chunk) {
		ArgumentChecker.checkForValidation(this);
		return flake_data_manager.getIndexOfChunk(chunk);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void addChunks(Collection<Chunk> chunk_collection) {
		ArgumentChecker.checkForValidation(this);
		flake_data_manager.addChunks(chunk_collection);
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
			close();
			flake_data_manager.recycle();
			is_deleted = true;
			return true;
		}
		if( !hasBeenOpened() ) {
			return false;
		}
		if( isOpen() ) {
			close();
		}
		flake_data_manager.recycle();
		is_deleted = true;
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
		if( !isValid() ) {
			return 0;
		}
		return flake_data_manager.getLength();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#setLength(long)
	 */
	@Override public void setLength(long new_length) {
		ArgumentChecker.checkForValidation(this);
		if( getLength() != new_length ) {
			flake_data_manager.setLength(new_length);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getNumberOfChunks()
	 */
	@Override public int getNumberOfChunks() {
		return flake_data_manager.getNumberOfChunks();
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
	@Override public IChunk[] getChunks() {
		ArgumentChecker.checkForValidation(this);
		return flake_data_manager.getChunks();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getChunkAtIndex(int)
	 */
	@Override public IChunk getChunkAtIndex(int index) {
		ArgumentChecker.checkForValidation(this);
		return flake_data_manager.getChunkAtIndex(index);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getChunkAtPositionInFlake(long)
	 */
	@Override public IChunk getChunkAtPositionInFlake(long position_in_flake) {
		ArgumentChecker.checkForValidation(this);
		return flake_data_manager.getChunkAtPosition(position_in_flake);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getFlakeInputStream()
	 */
	@Override public FlakeInputStream getFlakeInputStream() throws IOException {
		if( !isValid() ) {
			throw new SecurityException("The flake can not be streamed!");
		}
		return new FlakeInputStream(this, channel_manager.getChannel(), channel_manager);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getFlakeOutputStream()
	 */
	@Override public FlakeOutputStream getFlakeOutputStream() throws IOException {
		if( !isValid() ) {
			throw new SecurityException("The flake can not be streamed!");
		}
		return new FlakeOutputStream(this, channel_manager.getChannel(), channel_manager);
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
			flake_data_manager.arrangeChunks();
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
	
}
