package snowflake.core.manager;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.zip.CRC32;

import j3l.util.ArrayTool;
import j3l.util.ClosureState;
import j3l.util.check.ArgumentChecker;
import j3l.util.close.IClose;
import j3l.util.transform.TransformValue;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.12_0
 * @author Johannes B. Latzel
 */
public final class IndexManager implements IClose<IOException> {
	
	
	/**
	 * <p></p>
	 */
	private ClosureState closure_state;
	
	
	/**
	 * <p>a list of all available indices</p>
	 */
	private final LinkedList<Long> available_index_list;
	
	
	/**
	 * <p></p>
	 */
	private final Object index_lock;
	
	
	/**
	 * <p></p>
	 */
	private final Object configuration_lock;
	
	
	/**
	 * <p></p>
	 */
	private long next_index;
	
	
	/**
	 * <p></p>
	 */
	private final File configuration_file; 
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	protected IndexManager(File configuration_file) {
		
		ArgumentChecker.checkForExistence(configuration_file, "configuration_file");
		
		this.configuration_file = configuration_file;
		available_index_list = new LinkedList<>();
		index_lock = new Object();
		configuration_lock = new Object();
		next_index = 0;
		closure_state = ClosureState.None;
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	protected void loadConfiguration() throws IOException {
		
		
		if( hasBeenOpened() ) {
			return;
		}
		
			
		int read_in_checksum;
		CRC32 checksum = new CRC32();
		int number_of_indices;
		int position_in_input_buffer = 0;
		int bytes_in_buffer;
		int max_bytes_in_buffer;
		long length_of_file;
		long current_position = 0;
		long remaining_bytes_in_file;
		boolean skip_indices = false;
		
		// max 1024 indices per buffer
		int buffer_size = 8 * 1024;
		byte[] input_buffer = new byte[buffer_size];
		byte[] long_buffer = new byte[8];
		byte[] int_buffer = new byte[4];
		
		synchronized( configuration_lock ) {
			
			synchronized( index_lock ) {
				
				length_of_file = configuration_file.length();
				
				if( length_of_file == 0 ) {
					return;
				}
				else if( length_of_file < 16 ) {
					throw new SecurityException("The configuration file has an incomplete header!");
				}
				
				
				try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(configuration_file))) {
					
					
					input.read(long_buffer);
					current_position += long_buffer.length;
					synchronized( index_lock ) {
						next_index = TransformValue.toLong(long_buffer);
					}				
					checksum.update(long_buffer);
					
					input.read(int_buffer);
					current_position += int_buffer.length;
					number_of_indices = TransformValue.toInteger(int_buffer);
					checksum.update(int_buffer);
					
					input.read(int_buffer);
					current_position += int_buffer.length;
					read_in_checksum = TransformValue.toInteger(int_buffer);
					
					
					
					remaining_bytes_in_file = length_of_file - current_position;
					
					// checks if the number of indices matches the number of remaining bytes
					// also assures that the number of remaining bytes can be devided by 8
					if( remaining_bytes_in_file != number_of_indices * 8 || number_of_indices < 0 ) {
						throw new IOException("The number of indecies \"" + number_of_indices + "\" is invalid!");
					}
					else if( number_of_indices == 0 ) {
						skip_indices = true;
					}
					
					if( !skip_indices ) {
						
						while( remaining_bytes_in_file != 0 ) {			
							
							if( remaining_bytes_in_file >= input_buffer.length ) {
								max_bytes_in_buffer = input_buffer.length;
							}
							else {
								// cast is okay, because remaining_bytes_in_file is smaller than input_buffer.length which is int
								max_bytes_in_buffer = (int)(remaining_bytes_in_file);						
							}

							bytes_in_buffer = input.read(input_buffer, 0, max_bytes_in_buffer);
							checksum.update(input_buffer, 0, bytes_in_buffer);
							current_position += bytes_in_buffer;
							
							// will always have at least 8 bytes of data and at max buffer.length bytes
							// will always contain a number of bytes which can be divided by 8
							do {
								
								ArrayTool.transferValues(long_buffer, input_buffer, 0, 
										position_in_input_buffer, long_buffer.length);
								position_in_input_buffer += long_buffer.length;
								available_index_list.add(new Long(TransformValue.toLong(long_buffer)));
								
							}
							while( position_in_input_buffer != bytes_in_buffer );
							
							
							remaining_bytes_in_file = length_of_file - current_position;
							
						}
						
						
					}
					
					
					// cast is necessary, because the actual checksum returned is 32 bit integer
					if( read_in_checksum != (int)checksum.getValue() ) {
						throw new SecurityException("The read-in checksum \"" + read_in_checksum + "\" does not match "
								+ "the calculated checksum \"" + checksum.getValue() + "\"!");
					}
					
					
				}
				catch (IOException e) {
					throw new IOException("Can not read from the configuration-file \""
							+ configuration_file.getAbsolutePath() + "\" at position: " + current_position, e);
				}
				
				
			}			
			
		}
		
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	protected void saveConfiguration() throws IOException {
		
		
		if( !hasBeenOpened() || isClosed() ) {
			return;
		}
		
			
		CRC32 checksum = new CRC32();
		int number_of_indices;
		long current_position = 0;
		byte[] long_buffer = new byte[8];
		byte[] int_buffer = new byte[4];
		
		synchronized( configuration_lock ) {
			
			synchronized( index_lock ) {
				
				
				number_of_indices = available_index_list.size();
						
				try (RandomAccessFile output = new RandomAccessFile(configuration_file, "rw");) {
					
					// 16 header-bytes
					output.setLength( 16 + number_of_indices * 8 );
					
					output.seek(0);
					TransformValue.toByteArray(next_index, long_buffer);
					checksum.update(long_buffer);
					output.write(long_buffer);
					
					TransformValue.toByteArray(number_of_indices, int_buffer);
					checksum.update(int_buffer);
					output.write(int_buffer);
					
					// 16 is the position one past the header
					output.seek(16);
					
					if( number_of_indices != 0 ) {
						
						for(int a=0,n=available_index_list.size();a<n;a++) {
							
							TransformValue.toByteArray(available_index_list.get(a).longValue(), long_buffer);
							output.write(long_buffer);
							checksum.update(long_buffer);
							current_position++;
							
						}
						
					}
					
					// checksum writes in [12,16)
					output.seek(12);
					// cast is necessary, because the actual checksum returned is 32 bit integer
					output.write(TransformValue.toByteArray((int)checksum.getValue()));
					
					
				}
				catch (IOException e) {
					throw new IOException("Can not write to the configuration-file \""
							+ configuration_file.getAbsolutePath() + "\" at byte: " + current_position, e);
				}
				
			
			}
			
			
		}
		
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	protected void setNewIndex(long new_index) {
		
		if( !isOpen() ) {
			throw new SecurityException("The instance is not open!");
		}
		
		if( new_index < this.next_index ) {
			throw new IllegalArgumentException("The new index must not be smaller than the current index!");
		}
		else {
			if( new_index == this.next_index ) {
				return;
			}
		}
		
		this.next_index = new_index;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getAvailableIndex() {
		
		if( !isOpen() ) {
			throw new SecurityException("The instance is not open!");
		}
		
		synchronized( index_lock ) {
			if( available_index_list.size() != 0 ) {
				return available_index_list.removeLast().longValue();
			}
			else {
				long current_index = next_index;
				next_index++;
				return current_index;
			}
		}
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	protected void addAvailableIndex(long index) {
		
		if( !isOpen() ) {
			throw new SecurityException("The instance is not open!");
		}
		
		Long boxed_index = new Long(index);
		
		synchronized( index_lock ) {
			if( available_index_list.contains(boxed_index)) {
				throw new Error("An index must never exist more than once!");
			}
			available_index_list.add(boxed_index);
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	protected void clearAvailableIndices() {
		synchronized( index_lock ) {
			if( isOpen() ) {
				available_index_list.clear();
			}
		}
	}


	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IStateClosure#getClosureState()
	 */
	@Override public ClosureState getClosureState() {
		return closure_state;
	}


	/*
	 * (non-Javadoc)
	 * @see j3l.util.interfaces.IClose#onOpen()
	 */
	@Override public void open() throws IOException {
		
		if( hasBeenOpened() ) {
			return;
		}
		
		closure_state = ClosureState.InOpening;
		loadConfiguration();
		closure_state = ClosureState.Open;
		
	}


	/*
	 * (non-Javadoc)
	 * @see j3l.util.interfaces.IClose#onClose()
	 */
	@Override public void close() throws IOException {
		
		if( !isOpen() ) {
			return;
		}
		
		closure_state = ClosureState.InClosure;
		saveConfiguration();
		closure_state = ClosureState.Closed;
		
	}
	
}
