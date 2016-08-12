package snowflake.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.api.DataPointer;
import snowflake.core.manager.IReturnChannel;
import snowflake.core.storage.IRead;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.08.12_0
 * @author Johannes B. Latzel
 */
public final class FlakeInputStream implements ReadableByteChannel {
		
	
	/**
	 * <p></p>
	 */
	private final DataPointer data_pointer;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_closed;
	
	
	/**
	 * <p></p>
	 */
	private final IRead read;
	
	
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
	public FlakeInputStream(Flake flake, IRead read, IReturnChannel channel_returner) {
		if( StaticMode.TESTING_MODE ) {
			this.read = Checker.checkForNull(read, GlobalString.Read.toString());
			this.channel_returner = Checker.checkForNull(
				channel_returner, GlobalString.ChannelReturner.toString()
			);
		}
		else {
			this.read = read;
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
	
	
	/* (non-Javadoc)
	 * @see java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)
	 */
	@Override public int read(ByteBuffer buffer) throws IOException {
		if( is_closed ) {
			throw new IOException("The stream is not open!");
		}
		if( data_pointer.isEOF() ) {
			return -1;
		}
		return read.read(data_pointer, buffer);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long available() {
		if( is_closed ) {
			return 0;
		}
		return data_pointer.getRemainingBytes();
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see com.sun.xml.internal.ws.Closeable#close()
	 */
	@Override public void close() throws IOException {
		if( is_closed ) {
			return;
		}
		channel_returner.returnChannel(read);
		is_closed = true;
	}
	
	
	/* (non-Javadoc)
	 * @see java.nio.channels.Channel#isOpen()
	 */
	@Override public boolean isOpen() {
		return !is_closed;
	}
	
}
