package snowflake.filesystem.manager;

import java.util.ArrayList;
import java.util.HashMap;

import j3l.util.LoopedTaskThread;
import j3l.util.check.ArgumentChecker;
import snowflake.GlobalString;
import snowflake.filesystem.File;
import snowflake.filesystem.FileSystem;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.25_0
 * @author Johannes B. Latzel
 */
public final class DeduplicationManager {
	
	
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
	private final ArrayList<File> potential_file_list;
	
	
	/**
	 * <p></p>
	 */
	private final HashMap<Long, DeduplicationBlock> deduplication_block_map;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DeduplicationManager(FileSystem file_system) {
		this.file_system = ArgumentChecker.checkForNull(file_system, GlobalString.FileSystem.toString());
		potential_file_list = new ArrayList<>();
		this.deduplication_thread = new LoopedTaskThread(
			this::deduplicateFile,
			"Snowflake Deduplication Thread",
			10_000
		);
		deduplication_thread.start();
		this.analyzation_thread = new LoopedTaskThread(
			this::deduplicateFile,
			"Snowflake Deduplication Analyzation Thread",
			1_000_000
		);
		deduplication_thread.start();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void deduplicateFile() {
		
	}
	
}
