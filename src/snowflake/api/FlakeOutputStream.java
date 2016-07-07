package snowflake.api;

import java.io.IOException;
import java.io.OutputStream;

import j3l.util.check.ArgumentChecker;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.core.Flake;
import snowflake.core.manager.IReturnChannel;
import snowflake.core.storage.IWrite;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.01_0
 * @author Johannes B. Latzel
 */
public final class FlakeOutputStream extends OutputStream {
		
	
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
	private boolean is_closed;
	
	
	/**
	 * <p></p>
	 */
	private final IWrite write;
	
	
	/**
	 * <p></p>
	 */
	private final IReturnChannel channel_returner;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FlakeOutputStream(Flake flake, IWrite write, IReturnChannel channel_returner) {
		if( StaticMode.TESTING_MODE ) {
			this.flake = ArgumentChecker.checkForNull(flake, GlobalString.Flake.toString());
			this.write = ArgumentChecker.checkForNull(write, GlobalString.Write.toString());
			this.channel_returner = ArgumentChecker.checkForNull(
				channel_returner, GlobalString.ChannelReturner.toString()
			);
		}
		else {
			this.flake = flake;
			this.write = write;
			this.channel_returner = channel_returner;
		}
		data_pointer = new DataPointer(flake, 0L);
		is_closed = false;
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
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void trim() {
		flake.setLength(data_pointer.getPositionInFlake());
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void ensureCapacity(long number_of_bytes) {
		if( number_of_bytes > flake.getLength() ) {
			flake.setLength(number_of_bytes);
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void ensureRemainingCapacity(long number_of_bytes) {
		if( number_of_bytes > data_pointer.getRemainingBytes() ) {
			flake.setLength(data_pointer.getPositionInFlake() + number_of_bytes);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see java.io.OutputStream#write(int)
	 */
	@Override public void write(int b) throws IOException {
		if( is_closed ) {
			throw new IOException("The stream is not open!");
		}
		if( data_pointer.getRemainingBytes() == 0 ) {
			flake.setLength( flake.getLength() + 1 );
		}
		write.write(data_pointer, (byte)(b));	
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.io.OutputStream#write(byte[], int, int)
	 */
	@Override public void write(byte[] buffer, int offset, int length) throws IOException {
		if( is_closed ) {
			throw new IOException("The stream is not open!");
		}
		long remaining_bytes = data_pointer.getRemainingBytes();
		if( remaining_bytes < length ) {
			flake.setLength( flake.getLength() + length - remaining_bytes );
		}
		write.write(data_pointer, buffer, offset, length);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.io.OutputStream#close()
	 */
	@Override public synchronized void close() throws IOException {
		if( is_closed ) {
			return;
		}
		try {
			flush();
		}
		catch( IOException e ) {
			throw new IOException("Failed to flush the stream!", e);
		}
		channel_returner.returnChannel(write);
		is_closed = true;
	}
}
