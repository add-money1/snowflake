package snowflake.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import snowflake.core.Flake;
import snowflake.core.FlakeInputStream;
import snowflake.filesystem.File;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.08.12_0
 * @author Johannes B. Latzel
 */
public class FileInputStream implements ReadableByteChannel {
	
	
	/**
	 * <p></p>
	 */
	protected final FlakeInputStream flake_input_stream;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	public FileInputStream(File file, Flake data_flake) throws IOException {
		if( file.isInDeduplication() ) {
			throw new IOException("The file \"" + file + "\" is in deduplication!");
		}
		else if( file.isDeduplicated() ) {
			throw new IOException("The file \"" + file + "\" is deduplicated!");
		}
		flake_input_stream = data_flake.getFlakeInputStream();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public DataPointer getDataPointer() {
		return flake_input_stream.getDataPointer();
	}
	
	
	/* (non-Javadoc)
	 * @see java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)
	 */
	@Override public int read(ByteBuffer buffer) throws IOException {
		return flake_input_stream.read(buffer);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.io.InputStream#close()
	 */
	@Override public void close() throws IOException {
		flake_input_stream.close();
	}
	
	
	/* (non-Javadoc)
	 * @see java.nio.channels.Channel#isOpen()
	 */
	@Override public boolean isOpen() {
		return flake_input_stream.isOpen();
	}
	
}
