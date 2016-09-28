package snowflake.core;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.api.DataPointer;
import snowflake.core.storage.IRead;
import snowflake.core.storage.IWrite;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.08.12_0
 * @author Johannes B. Latzel
 */
public final class Channel implements IRead, IWrite, Closeable {
	
	
	/**
	 * <p></p>
	 */
	private final RandomAccessFile data_file;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws FileNotFoundException 
	 */
	public Channel(RandomAccessFile data_file) {
		if( StaticMode.TESTING_MODE ) {
			this.data_file = Checker.checkForNull(data_file, GlobalString.DataFile.toString());
		}
		else {
			this.data_file = data_file;
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.storage.IWrite#write(snowflake.api.DataPointer, java.nio.ByteBuffer)
	 */
	@Override public void write(DataPointer data_pointer, ByteBuffer buffer) throws IOException {
		if( !data_file.getChannel().isOpen() ) {
			throw new IOException("The chanel is not open!");
		}
		byte[] byte_array_buffer = new byte[buffer.remaining()];
		buffer.get(byte_array_buffer);		
		int remaining_bytes = byte_array_buffer.length;
		int advance_in_buffer;
		int remaining_bytes_in_chunk;
		long actual_remaining_bytes_in_chunk;
		if( data_pointer.getRemainingBytes() < remaining_bytes ) {
			throw new IndexOutOfBoundsException("The length must not succeed the number of available bytes in the flake!");
		}
		do {
			data_file.seek(data_pointer.getPositionInStorage());
			actual_remaining_bytes_in_chunk = data_pointer.getRemainingBytesInChunk();
			if( actual_remaining_bytes_in_chunk >= Integer.MAX_VALUE ) {
				remaining_bytes_in_chunk = Integer.MAX_VALUE;
			}
			else {
				// cast to int is okay, because actual_remaining_bytes_in_chunk < Integer.MAX_VALUE
				remaining_bytes_in_chunk = (int)actual_remaining_bytes_in_chunk;
			}
			advance_in_buffer = Math.min(remaining_bytes, remaining_bytes_in_chunk);
			data_file.write(byte_array_buffer, byte_array_buffer.length - remaining_bytes, advance_in_buffer);
			remaining_bytes -= advance_in_buffer;
			data_pointer.changePosition(advance_in_buffer);
		}
		while( remaining_bytes != 0 );
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.storage.IRead#read(snowflake.api.DataPointer, java.nio.ByteBuffer)
	 */
	@Override public int read(DataPointer data_pointer, ByteBuffer buffer) throws IOException {
		if( !data_file.getChannel().isOpen() ) {
			throw new IOException("The chanel is not open!");
		}
		byte[] byte_array_buffer = new byte[buffer.remaining()];
		int remaining_bytes;
		if( data_pointer.getRemainingBytes() < Integer.MAX_VALUE ) {
			// cast ok, because data_pointer.getRemainingBytes() < Integer.MAX_VALUE
			remaining_bytes = Math.min(byte_array_buffer.length, (int)data_pointer.getRemainingBytes());
		}
		else {
			remaining_bytes = Math.min(byte_array_buffer.length, Integer.MAX_VALUE);
		}
		if( remaining_bytes == 0 ) {
			return 0;
		}
		int read_in_bytes = 0;
		int current_read_in_bytes;
		int advance_in_buffer;
		int remaining_bytes_in_chunk;
		long actual_remaining_bytes_in_chunk;
		do {
			data_file.seek(data_pointer.getPositionInStorage());
			actual_remaining_bytes_in_chunk = data_pointer.getRemainingBytesInChunk();
			if( actual_remaining_bytes_in_chunk >= Integer.MAX_VALUE ) {
				remaining_bytes_in_chunk = Integer.MAX_VALUE;
			}
			else {
				// cast to int is okay, because actual_remaining_bytes_in_chunk < Integer.MAX_VALUE
				remaining_bytes_in_chunk = (int)actual_remaining_bytes_in_chunk;
			}
			advance_in_buffer = Math.min(remaining_bytes, remaining_bytes_in_chunk);
			current_read_in_bytes = data_file.read(
				byte_array_buffer, byte_array_buffer.length - remaining_bytes, advance_in_buffer
			);
			if( current_read_in_bytes < 0 ) {
				return read_in_bytes;
			}
			remaining_bytes -= current_read_in_bytes;
			data_pointer.changePosition(current_read_in_bytes);
			read_in_bytes += current_read_in_bytes;
		}
		while( remaining_bytes != 0 );
		buffer.put(byte_array_buffer);
		return read_in_bytes;
	}
	
	
	/* (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override public void close() throws IOException {
		data_file.close();
		System.out.println("Channel: close()");
	}
	
}
