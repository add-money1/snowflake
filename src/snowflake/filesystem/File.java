package snowflake.filesystem;

import java.io.IOException;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.api.CommonAttribute;
import snowflake.api.FileSystemException;
import snowflake.api.IAttributeValue;
import snowflake.api.IDirectory;
import snowflake.api.IFlake;
import snowflake.core.FlakeInputStream;
import snowflake.core.FlakeOutputStream;
import snowflake.filesystem.attribute.DeduplicationDescription;
import snowflake.filesystem.attribute.DeduplicationProgressDescription;
import snowflake.filesystem.manager.IDeduplicationDescription;
import snowflake.filesystem.manager.IDeduplicationProgressDescription;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.22_0
 * @author Johannes B. Latzel
 */
public final class File extends Node {
	
	
	/**
	 * <p></p>
	 */
	private final IFlake data_flake;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public File(IFlake attribute_flake, IFlake data_flake, IDirectory parent_directory, long index) {
		super(attribute_flake, parent_directory, index);
		this.data_flake = Checker.checkForValidation(data_flake, GlobalString.DataFlake.toString());
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FlakeInputStream getFlakeInputStream() throws IOException {
		return data_flake.getFlakeInputStream();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FlakeOutputStream getFlakeOutputStream() throws IOException  {
		return getFlakeOutputStream(null);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FlakeOutputStream getFlakeOutputStream(Lock lock) throws IOException {
		if( isLocked() && (lock == null || !isLockedBy(lock)) ) {
			throw new IOException("The file is locked!");
		}
		return data_flake.getFlakeOutputStream();
	}
	
	
	/**
	 * @return
	 */
	public long getDataFlakeIdentification() {
		return data_flake.getIdentification();
	}
	
	
	/**
	 * @return
	 */
	public boolean isEmpty() {
		return getLength() == 0;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getLength() {
		return data_flake.getLength();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final IDeduplicationDescription getDeduplicationDescription() {
		IAttributeValue<?> attribute_value = null;
		if( hasAttribute(CommonAttribute.DeduplicationDescription) ) {
			attribute_value = getAttribute(CommonAttribute.DeduplicationDescription).getAttributeValue();
			if( attribute_value instanceof DeduplicationDescription ) {
				return ((DeduplicationDescription)attribute_value).getValue();
			}
		}
		throw new FileSystemException("The file has no deduplication_description!");
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final IDeduplicationProgressDescription getDeduplicationProgressDescription() {
		IAttributeValue<?> attribute_value = null;
		if( hasAttribute(CommonAttribute.DeduplicationProgressDescription) ) {
			attribute_value = getAttribute(CommonAttribute.DeduplicationProgressDescription).getAttributeValue();
			if( attribute_value instanceof DeduplicationProgressDescription ) {
				return ((DeduplicationProgressDescription)attribute_value).getValue();
			}
		}
		throw new FileSystemException("The file has no deduplication_progress_description!");
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean isDeduplicated() {
		return hasAttribute(CommonAttribute.DeduplicationDescription);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean isInDeduplication() {
		return hasAttribute(CommonAttribute.DeduplicationProgressDescription);
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.check.IValidate#isValid()
	 */
	@Override public boolean isValid() {
		// data_flake is null if the object is still in construction (e.g. the constructor is processed)
		if( data_flake == null ) {
			// must return true, since only valid Nodes can be added to an IDirectory
			return true;
		}
		return super.isValid() && data_flake.isValid();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.filesystem.Node#reactToDeletion()
	 */
	@Override protected void reactToDeletion() {
		data_flake.delete();
	}

}
