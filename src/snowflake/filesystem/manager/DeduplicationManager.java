package snowflake.filesystem.manager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;

import j3l.util.ArrayTool;
import j3l.util.Checker;
import j3l.util.ClosureState;
import j3l.util.IClose;
import j3l.util.LoopedTaskThread;
import j3l.util.TransformValue2;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.Util;
import snowflake.api.CommonAttribute;
import snowflake.api.DataPointer;
import snowflake.api.FileSystemException;
import snowflake.api.IDirectory;
import snowflake.api.IFlake;
import snowflake.api.StorageException;
import snowflake.core.FlakeInputStream;
import snowflake.core.FlakeOutputStream;
import snowflake.core.storage.ICreateFlake;
import snowflake.core.storage.IGetFlake;
import snowflake.filesystem.Attribute;
import snowflake.filesystem.File;
import snowflake.filesystem.FileSystem;
import snowflake.filesystem.Lock;
import snowflake.filesystem.Node;
import snowflake.filesystem.attribute.DededuplicationProgressDescription;
import snowflake.filesystem.attribute.DeduplicationDescription;
import snowflake.filesystem.attribute.DeduplicationProgressDescription;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.22_0
 * @author Johannes B. Latzel
 */
public final class DeduplicationManager implements IClose<FileSystemException> {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private static void copyData(File file, IFlake flake) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(file, GlobalString.File.toString());
			Checker.checkForNull(flake, GlobalString.Flake.toString());
		}
		DataPointer input_pointer;
		ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
		try( FlakeInputStream fin = file.getFlakeInputStream() ) {
			input_pointer = fin.getDataPointer();
			try( FlakeOutputStream fout = flake.getFlakeOutputStream() ) {
				while( !input_pointer.isEOF() ) {
					fin.read(buffer);
					buffer.flip();
					fout.write(buffer);
					buffer.rewind();
				}
			}
			catch( IOException e ) {
				throw e;
			}
		}
		catch( IOException e ) {
			throw new StorageException(
				"Could not copy the file \"" + file.toString() + "\" to the flake \"" + flake.toString() + "\"!", e
			);
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private static ByteBuffer getByteBuffer(LongBuffer long_buffer) {
		byte[] buffer = new byte[ Long.BYTES * long_buffer.remaining() ];
		byte[] l_buffer = new byte[Long.BYTES];
		int current_index = 0;
		while( long_buffer.hasRemaining() ) {
			ArrayTool.transferValues(
				buffer,
				TransformValue2.toByteArray(long_buffer.get(), l_buffer),
				current_index,
				0,
				l_buffer.length,
				StaticMode.TESTING_MODE
			);
			current_index += l_buffer.length;
		}
		return ByteBuffer.wrap(buffer);
	}
	
	
	/**
	 * <p></p>
	 */
	private final LoopedTaskThread deduplication_thread;
	
	
	/**
	 * <p></p>
	 */
	private final LoopedTaskThread analyzation_thread;
	
	
	/**
	 * <p></p>
	 */
	private final FileSystem file_system;
	
	
	/**
	 * <p></p>
	 */
	private final ArrayList<File> deduplicant_list;
	
	
	/**
	 * <p></p>
	 */
	private final DeduplicationTable deduplication_table;
	
	
	/**
	 * <p></p>
	 */
	private ClosureState closure_state;
	
	
	/**
	 * <p></p>
	 */
	private final ICreateFlake create_flake;
	
	
	/**
	 * <p></p>
	 */
	private final IGetFlake get_flake;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DeduplicationManager(FileSystem file_system, IFlake deduplication_table_flake,
										ICreateFlake create_flake, IGetFlake get_flake) {
		if( StaticMode.TESTING_MODE ) {
			this.file_system = Checker.checkForNull(file_system, GlobalString.FileSystem.toString());
			this.create_flake = Checker.checkForNull(create_flake, GlobalString.CreateFlake.toString());
			this.get_flake = Checker.checkForNull(get_flake, GlobalString.GetFlake.toString());
		}
		else {
			this.file_system = file_system;
			this.create_flake = create_flake;
			this.get_flake = get_flake;
		}
		deduplicant_list = new ArrayList<>();
		try {
			deduplication_table = new DeduplicationTable(deduplication_table_flake);
		}
		catch( IOException e ) {
			throw new FileSystemException("Can not instantiate the deduplication_table!", e);
		}
		this.deduplication_thread = new LoopedTaskThread(
			this::deduplicateNextFile,
			"Snowflake Deduplication Thread",
			1_000
		);
		this.analyzation_thread = new LoopedTaskThread(
			this::analyzeFileSystem,
			"Snowflake Deduplication Analyzation Thread",
			1_000_000
		);
		this.closure_state = ClosureState.None;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void deduplicateNextFile() {
		File file;
		synchronized( deduplicant_list ) {
			while( deduplicant_list.isEmpty() ) {
				try {
					deduplicant_list.wait();
				}
				catch( InterruptedException e ) {
					e.printStackTrace();
				}
			}
			file = deduplicant_list.remove(deduplicant_list.size() - 1);
		}
		deduplicateFile(file);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void analyzeFileSystem() {
		ArrayList<File> new_deduplicant_list = new ArrayList<>(10_000);
		ArrayList<IDirectory> next_directory_list = new ArrayList<>(10_000);
		ArrayList<Node> next_child_node_list = new ArrayList<>(1_000);
		next_child_node_list.addAll(file_system.getRootDirectory().getChildNodes());
		Node current_node;
		File current_file;
		while( !next_directory_list.isEmpty()  ) {
			if( next_child_node_list.isEmpty() ) {
				next_child_node_list.addAll(
					next_directory_list.remove(next_directory_list.size() - 1).getChildNodes()
				);
				// continue is import, because the removal in the below code could cause a crash
				continue;
			}
			current_node = next_child_node_list.remove(next_child_node_list.size() - 1);
			if( current_node instanceof IDirectory ) {
				// cast is okay, because current_node is an instance of IDirectory
				next_directory_list.add((IDirectory)current_node);
			}
			else if( current_node instanceof File ) {
				// cast is okay, because current_node is an instance of File
				current_file = (File)current_node;
				if( isPotentialDeduplicant(current_file) ) {
					new_deduplicant_list.add(current_file);
				}
			}
		}
		if( !new_deduplicant_list.isEmpty() ) {
			synchronized( deduplicant_list ) {
				deduplicant_list.addAll(new_deduplicant_list);
				deduplicant_list.notifyAll();
			}
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean isPotentialDeduplicant(File file) {
		// assumes all undeduplicated files are okay
		return file != null && deduplication_table != null && !file.isDeduplicated();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void deduplicateFile(File file) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForValidation(file, GlobalString.File.toString());
		}
		Lock lock = file.lock();
		if( file.isDeduplicated() ) {
			file.unlock(lock);
			return;
		}
		final IDeduplicationProgressDescription deduplication_progress_description;
		long current_index_pointer;
		if( file.isInDeduplication() ) {
			deduplication_progress_description = file.getDeduplicationProgressDescription();
			current_index_pointer = deduplication_progress_description.getCurrentIndexPointer();
		}
		else {
			current_index_pointer = 0L;
			final DeduplicationProgressDescription dedup_descr = new DeduplicationProgressDescription(
				0L, current_index_pointer
			);
			deduplication_progress_description = dedup_descr;
			file.setAttribute(
				new Attribute(CommonAttribute.DeduplicationProgressDescription, dedup_descr)
			);
		}
		final DataPointer current_data_pointer;
		long previous_current_index_pointer = current_index_pointer;
		// fills at max one deduplication_block-sized block of data with indices, so that
		// it never accidentally overwrites not yet processed data
		LongBuffer long_buffer = LongBuffer.allocate( DeduplicationBlock.SIZE / Long.BYTES );
		final ByteBuffer deduplication_block_buffer = ByteBuffer.allocate(DeduplicationBlock.SIZE);
		final long file_length = file.getLength();
		try( FlakeInputStream fin = file.getFlakeInputStream() ) {
			current_data_pointer = fin.getDataPointer();
			current_data_pointer.setPosition(deduplication_progress_description.getCurrentDataPointer());
			while( true ) {
				if( !long_buffer.hasRemaining() || current_data_pointer.isEOF() ) {
					long_buffer.flip();
					try( FlakeOutputStream fout = file.getFlakeOutputStream(lock) ) {
						fout.getDataPointer().setPosition(previous_current_index_pointer);
						fout.write(getByteBuffer(long_buffer));
						previous_current_index_pointer = current_index_pointer;
					}
					catch( IOException e ) {
						throw new IOException("Can not write to the FlakeOutputStream!", e);
					}
					file.setAttribute(
						new Attribute(
							CommonAttribute.DeduplicationProgressDescription,
							new DeduplicationProgressDescription(
								current_data_pointer.getPositionInFlake(),
								current_index_pointer
							)
						),
						lock
					);
					if( current_data_pointer.isEOF() ) {
						break;
					}
					long_buffer.rewind();
				}
				if( current_data_pointer.getRemainingBytes() < deduplication_block_buffer.capacity() ) {
					current_data_pointer.seekEOF();
					continue;
				}
				Util.readComplete(fin, deduplication_block_buffer);
				long index = deduplication_table.getIndex(deduplication_block_buffer);
				if( index != -1 ) {
					long_buffer.put(deduplication_table.getIndex(deduplication_block_buffer));
				}
				else {
					long_buffer.put(deduplication_table.register(deduplication_block_buffer));
				}
			}
			file.setAttribute(
				new Attribute(
					CommonAttribute.DeduplicationDescription, 
					new DeduplicationDescription(
						file_length - ( file_length % DeduplicationBlock.SIZE )
					)
				), 
				lock
			);
			file.removeAttribute(CommonAttribute.DeduplicationProgressDescription, lock);
		}
		catch( IOException e ) {
			e.printStackTrace();
		}
		finally {
			file.unlock(lock);
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void dededuplicateFile(File file) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForValidation(file, GlobalString.File.toString());
		}
		Lock lock = file.lock();
		if( !file.isDeduplicated() || file.isInDeduplication() ) {
			file.unlock(lock);
			return;
		}
		final IDededuplicationProgressDescription dededuplication_progress_description;
		long current_index_pointer;
		IFlake backup_flake;
		if( file.isInDededuplication() ) {
			dededuplication_progress_description = file.getDededuplicationProgressDescription();
			current_index_pointer = dededuplication_progress_description.getCurrentIndexPointer();
			backup_flake = get_flake.getFlake(dededuplication_progress_description.getBackupFlakeIdentification());
		}
		else {
			current_index_pointer = 0L;
			backup_flake = create_flake.createFlake();
			final DededuplicationProgressDescription dededup_descr = new DededuplicationProgressDescription(
				current_index_pointer, backup_flake.getIdentification()
			);
			dededuplication_progress_description = dededup_descr;
			try {
				DeduplicationManager.copyData(file, backup_flake);
			}
			catch( StorageException e ) {
				backup_flake.delete();
				throw new StorageException("Could not dededuplicate file \"" + file.toString() + "\"!", e);
			}
			file.setAttribute(
				new Attribute(CommonAttribute.DededuplicationProgressDescription, dededup_descr)
			);
		}
		final DataPointer input_pointer;
		DeduplicationBlock current_block;
		ByteBuffer index_buffer = ByteBuffer.allocateDirect(Long.BYTES * 1_000);
		try( FlakeInputStream fin = backup_flake.getFlakeInputStream() ) {
			input_pointer = fin.getDataPointer();
			try( FlakeOutputStream fout = file.getFlakeOutputStream(lock) ) {
				while( !input_pointer.isEOF() ) {
					fin.read(index_buffer);
					index_buffer.flip();
					while( index_buffer.hasRemaining() ) {
						current_block = deduplication_table.getDataBlock(index_buffer.getLong());
						fout.write(current_block.getBlockBuffer());
						// clears the cached block immediately so that the ram does not overflow..
						current_block.clearWhenUnusedFor(0);
					}
					index_buffer.rewind();
				}
			}
			catch( IOException e ) {
				throw e;
			}
			file.removeAttribute(CommonAttribute.DeduplicationDescription, lock);
			file.removeAttribute(CommonAttribute.DededuplicationProgressDescription, lock);
		}
		catch( IOException e ) {
			throw new StorageException("Could not dededuplicate the file \"" + file.toString() + "\"!", e);
		}
		finally {
			file.unlock(lock);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.close.IStateClosure#getClosureState()
	 */
	@Override public ClosureState getClosureState() {
		return closure_state;
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.close.IClose#open()
	 */
	@Override public synchronized void open() {
		if( isOpen() ) {
			if( StaticMode.TESTING_MODE ) {
				throw new FileSystemException("The DeduplicationManager has already been opened!");
			}
			return;
		}
		closure_state = ClosureState.InOpening;
		analyzation_thread.start();
		deduplication_thread.start();
		closure_state = ClosureState.Open;
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.close.IClose#close()
	 */
	@Override public synchronized void close() {
		if( !isOpen() ) {
			if( StaticMode.TESTING_MODE ) {
				throw new FileSystemException("The DeduplicationManager is not open!");
			}
			return;
		}
		closure_state = ClosureState.InClosure;
		analyzation_thread.interrupt();
		deduplication_thread.interrupt();
		closure_state = ClosureState.Closed;
	}
	
}
