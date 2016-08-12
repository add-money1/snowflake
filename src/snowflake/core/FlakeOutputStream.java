package snowflake.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.api.DataPointer;
import snowflake.core.manager.IReturnChannel;
import snowflake.core.storage.IWrite;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.08.12_0
 * @author Johannes B. Latzel
 */
public final class FlakeOutputStream implements WritableByteChannel {
		
	
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
			this.flake = Checker.checkForNull(flake, GlobalString.Flake.toString());
			this.write = Checker.checkForNull(write, GlobalString.Write.toString());
			this.channel_returner = Checker.checkForNull(
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
	 * @see java.nio.channels.WritableByteChannel#write(java.nio.ByteBuffer)
	 */
	@Override public int write(ByteBuffer buffer) throws IOException {
		if( is_closed ) {
			throw new IOException("The stream is not open!");
		}
		long remaining_bytes = data_pointer.getRemainingBytes();
		int length = buffer.capacity();
		if( remaining_bytes < length ) {
			flake.setLength( flake.getLength() + length - remaining_bytes );
		}
		write.write(data_pointer, buffer);
		return length;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override public synchronized void close() throws IOException {
		if( is_closed ) {
			return;
		}
		channel_returner.returnChannel(write);
		is_closed = true;
	}
	
	
	/* (non-Javadoc)
	 * @see java.nio.channels.Channel#isOpen()
	 */
	@Override public boolean isOpen() {
		return !is_closed;
	}
	
}
