package snowflake.core.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import j3l.util.check.ArgumentChecker;
import snowflake.api.GlobalString;
import snowflake.api.flake.DataPointer;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.04.06_0
 * @author Johannes B. Latzel
 */
public final class IOAccess implements IRead, IWrite {
	
	
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
	public IOAccess(RandomAccessFile data_file) {
		this.data_file = ArgumentChecker.checkForNull(data_file, GlobalString.DataFile.toString());
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IWrite#write(snowflake.api.DataPointer, byte)
	 */
	@Override public void write(DataPointer data_pointer, byte b) throws IOException {
		if( data_pointer.getRemainingBytes() < 1 ) {
			throw new IndexOutOfBoundsException("The length must not succeed the number of available bytes in the flake!");
		}
		data_file.seek(data_pointer.getPositionInStorage());
		data_file.write(b);
		data_pointer.increasePosition();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IWrite#write(snowflake.api.DataPointer, byte[], int, int)
	 */
	@Override public void write(DataPointer data_pointer, byte[] buffer, int offset, int length) throws IOException {
		int remaining_bytes = length;
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
			data_file.write(buffer, length - remaining_bytes + offset, advance_in_buffer);
			remaining_bytes -= advance_in_buffer;
			data_pointer.changePosition(advance_in_buffer);
		}
		while( remaining_bytes != 0 );
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IRead#read(snowflake.api.DataPointer)
	 */
	@Override public byte read(DataPointer data_pointer) throws IOException {
		if( data_pointer.isEOF() ) {
			throw new IOException("Can not read from a eof-stated stream!");
		}
		data_file.seek(data_pointer.getPositionInStorage());
		data_pointer.increasePosition();
		return (byte)data_file.read();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IRead#read(snowflake.api.DataPointer, byte[], int, int)
	 */
	@Override public int read(DataPointer data_pointer, byte[] buffer, int offset, int length) throws IOException {
		int remaining_bytes;
		if( data_pointer.getRemainingBytes() < Integer.MAX_VALUE ) {
			// cast ok, because data_pointer.getRemainingBytes() < Integer.MAX_VALUE
			remaining_bytes = Math.min(length, (int)data_pointer.getRemainingBytes());
		}
		else {
			remaining_bytes = Math.min(length, Integer.MAX_VALUE);
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
				buffer, length - remaining_bytes + offset, advance_in_buffer
			);
			if( current_read_in_bytes < 0 ) {
				return read_in_bytes;
			}
			remaining_bytes -= current_read_in_bytes;
			data_pointer.changePosition(current_read_in_bytes);
			read_in_bytes += current_read_in_bytes;
		}
		while( remaining_bytes != 0 );
		return read_in_bytes;
	}
	
	
	/* (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override public void close() throws IOException {
		data_file.close();
	}
	
}
