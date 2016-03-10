package snowflake.api.stream;

import java.io.IOException;
import java.io.OutputStream;

import j3l.util.check.ArgumentChecker;
import j3l.util.close.ClosureState;
import j3l.util.close.IClose;
import snowflake.api.GlobalString;
import snowflake.api.flake.DataPointer;
import snowflake.core.flake.Flake;
import snowflake.core.flake.IFlakeStreamManager;
import snowflake.core.storage.IWrite;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.03.10_0
 * @author Johannes B. Latzel
 */
public final class FlakeOutputStream extends OutputStream implements IClose<IOException> {
		
	
	/**
	 * <p></p>
	 */
	private final DataPointer data_pointer;
	
	
	/**
	 * <p></p>
	 */
	private final Flake flake;
	
	
	/**
	 * <p></p>
	 */
	private ClosureState closure_state;
	
	
	/**
	 * <p></p>
	 */
	private final IWrite write;
	
	
	/**
	 * <p></p>
	 */
	private final IFlakeStreamManager flake_stream_manager;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FlakeOutputStream(Flake flake, IWrite write, IFlakeStreamManager flake_stream_manager) {
		this.flake = ArgumentChecker.checkForNull(flake, GlobalString.Flake.toString());
		this.write = ArgumentChecker.checkForNull(write, GlobalString.Write.toString());
		this.flake_stream_manager = ArgumentChecker.checkForNull(
			flake_stream_manager, GlobalString.FlakeStreamManager.toString()
		);
		data_pointer = new DataPointer(flake, 0L);
		closure_state = ClosureState.None;
		open();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public DataPointer getDataPointer() {
		return data_pointer;
	}
	
	
	/* (non-Javadoc)
	 * @see java.io.OutputStream#write(int)
	 */
	@Override public void write(int b) throws IOException {
		if( !isOpen() ) {
			throw new IOException("The stream is not open!");
		}
		else {
			if( data_pointer.getRemainingBytes() == 0 ) {
				flake.setLength( flake.getLength() + 1 );
			}
			write.write(data_pointer, (byte)(b));
		}		
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.io.OutputStream#write(byte[], int, int)
	 */
	@Override public void write(byte[] buffer, int offset, int length) throws IOException {
		if( !isOpen() ) {
			throw new IOException("The stream is not open!");
		}
		else {
			long remaining_bytes = data_pointer.getRemainingBytes();
			if( remaining_bytes < length ) {
				flake.setLength( flake.getLength() + length - remaining_bytes );
			}
			write.write(data_pointer, buffer, offset, length);
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
		if( !hasBeenOpened() ) {
			closure_state = ClosureState.Open;
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.io.OutputStream#close()
	 */
	@Override public void close() throws IOException {
		
		if( !isOpen() ) {
			return;
		}
		
		closure_state = ClosureState.InClosure;
		
		try {
			flush();
		}
		catch( IOException e ) {
			throw new IOException("Failed to flush the stream!", e);
		}
		finally {
			closure_state = ClosureState.Closed;
			flake_stream_manager.removeStream(this);
		}
		
	}
}
