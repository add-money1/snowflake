package snowflake.api.stream;

import java.io.IOException;
import java.io.InputStream;

import j3l.util.check.ArgumentChecker;
import snowflake.api.GlobalString;
import snowflake.api.flake.DataPointer;
import snowflake.core.flake.Flake;
import snowflake.core.flake.IRemoveStream;
import snowflake.core.storage.IRead;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.04.06_0
 * @author Johannes B. Latzel
 */
public final class FlakeInputStream extends InputStream {
		
	
	/**
	 * <p></p>
	 */
	private final DataPointer data_pointer;
	
	
	/**
	 * <p></p>
	 */
	private long marked_position_in_flake;
	
	
	/**
	 * <p></p>
	 */
	private int read_in_bytes_since_marking;
	
	
	/**
	 * <p></p>
	 */
	private int read_in_bytes_since_marking_limit;
	
	
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
	private final IRemoveStream flake_stream_manager;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FlakeInputStream(Flake flake, IRead read, IRemoveStream flake_stream_manager) {
		this.read = ArgumentChecker.checkForNull(read, GlobalString.Read.toString());
		this.flake_stream_manager = ArgumentChecker.checkForNull(
			flake_stream_manager, GlobalString.FlakeStreamManager.toString()
		);
		data_pointer = new DataPointer(flake, 0L);
		is_closed = false;
		mark(0);
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
	 * @see java.io.InputStream#read()
	 */
	@Override public int read() throws IOException {
		if( is_closed ) {
			throw new IOException("The stream is not open!");
		}
		return read.read(data_pointer);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.io.InputStream#read(byte[], int, int)
	 */
	@Override public int read(byte[] buffer, int offset, int length) throws IOException {
		if( is_closed ) {
			throw new IOException("The stream is not open!");
		}
		return read.read(data_pointer, buffer, offset, length);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.io.InputStream#skip(long)
	 */
	@Override public long skip(long number_of_bytes) {
		if( number_of_bytes <= 0  || is_closed ) {
			return 0;
		}
		long remaining_bytes = data_pointer.getRemainingBytes();
		if( number_of_bytes >= remaining_bytes ) {
			data_pointer.seekEOF();
			return remaining_bytes;
		}
		else {
			data_pointer.changePosition(number_of_bytes);
			return number_of_bytes;
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.io.InputStream#available()
	 */
	@Override public int available() {
		if( is_closed ) {
			return 0;
		}
		// cast is okay, because this method only returns an estimate
		return (int)data_pointer.getRemainingBytes();
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.io.InputStream#close()
	 */
	@Override public void close() throws IOException {
		try {
			read.close();
		}
		catch( IOException e ) {
			throw new IOException("Failed to close the " + GlobalString.Read.toString()+ "!", e);
		}
		flake_stream_manager.removeStream(this);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.io.InputStream#mark(int)
	 */
	@Override public synchronized void mark(int read_limit) {
		
		if( read_limit < 0 || is_closed ) {
			return;
		}
		
		marked_position_in_flake = data_pointer.getPositionInFlake();
		read_in_bytes_since_marking = 0;
		read_in_bytes_since_marking_limit = read_limit;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.io.InputStream#reset()
	 */
	@Override public synchronized void reset() throws IOException {
		
		if( read_in_bytes_since_marking > read_in_bytes_since_marking_limit ) {
			throw new IOException("The current mark has been invalidated due to the read_in_bytes_since_marking_limit.");
		}
		
		data_pointer.setPosition(marked_position_in_flake);
		mark(read_in_bytes_since_marking_limit);
		
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.io.InputStream#markSupported()
	 */
	@Override public boolean markSupported() {
		return true;
	}
	
}