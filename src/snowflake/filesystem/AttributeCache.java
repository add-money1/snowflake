package snowflake.filesystem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import j3l.util.check.ArgumentChecker;
import j3l.util.check.IValidate;
import snowflake.GlobalString;
import snowflake.api.IFlake;
import snowflake.api.StorageException;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.16_0
 * @author Johannes B. Latzel
 */
public final class AttributeCache implements IValidate {
	
	
	/**
	 * <p></p>
	 */
	private final ArrayList<Attribute> attribute_list;
	
	
	/**
	 * <p></p>
	 */
	private final IFlake attribute_flake;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private boolean is_deleted;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public AttributeCache(IFlake attribute_flake) {
		this.attribute_flake = ArgumentChecker.checkForValidation(
			attribute_flake, GlobalString.AttributeFlake.toString()
		);
		attribute_list = new ArrayList<>();
		is_deleted = false;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void loadAttribute(String name) {
		checkDeletion();
		ArgumentChecker.checkForEmptyString(name, GlobalString.Name.toString());
		synchronized( attribute_list ) {
			if( containsAttribute(name) ) {
				return;
			}
			synchronized( attribute_flake ) {
				Attribute attribute_wrapper = AttributeUtility.loadAttribute(name, attribute_flake);
				if( attribute_wrapper == null ) {
					throw new StorageException("The attribute \"" + name + "\"does not exist!");
				}
				attribute_list.add(attribute_wrapper);
			}
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void loadAttributes() {
		checkDeletion();
		boolean is_empty;
		synchronized( attribute_list ) {
			is_empty = attribute_list.isEmpty();
		}
		if( is_empty ) {
			loadAllAttributes();
		}
		else {
			loadNecessaryAttributes();
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void loadAllAttributes() {
		checkDeletion();
		synchronized( attribute_list ) {
			List<Attribute> loaded_attribute_list;
			synchronized( attribute_flake ) {
				loaded_attribute_list = AttributeUtility.loadAllAttributes(attribute_flake);
			}
			attribute_list.clear();
			attribute_list.addAll(loaded_attribute_list);
		} 
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void loadNecessaryAttributes() {
		checkDeletion();
		synchronized( attribute_list ) {
			List<Attribute> loaded_attribute_list;
			synchronized( attribute_flake ) {
				loaded_attribute_list = AttributeUtility.loadNecessaryAttributes(attribute_flake, attribute_list);
			}
			attribute_list.addAll(loaded_attribute_list);
		}
	}
	
	
	/**
	 * <p>not thread safe - must be called inside a synchronized( attribtue_list) block!</p>
	 *
	 * @param
	 * @return
	 */
	private boolean containsAttribute(String name) {
		if( attribute_list.isEmpty() ) {
			return false;
		}
		attribute_list.sort((l, r) -> 
			l.getName().compareTo(r.getName())
		);
		return attribute_list.stream().anyMatch(w -> w.getName().equals(name));
	}
	
	
	/**
	 * <p>O(n)</p>
	 *
	 * @param
	 * @return
	 */
	public Attribute getAttribute(String name) {
		checkDeletion();
		ArgumentChecker.checkForEmptyString(name, GlobalString.Name.toString());
		loadAttribute(name);
		synchronized( attribute_list ) {
			for( Attribute attribute : attribute_list ) {
				if( name.equals(attribute.getName()) ) {
					return attribute;
				}
			}
		}
		throw new NoSuchElementException("There is no attribute with the name \"" + name + "\"!");
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Collection<Attribute> getAttributes() {
		checkDeletion();
		loadAttributes();
		synchronized( attribute_list ) {
			return new ArrayList<>(attribute_list);
		}
	}
	
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setAttribute(Attribute attribute) {
		checkDeletion();
		ArgumentChecker.checkForNull(attribute, GlobalString.Attribute.toString());
		synchronized( attribute_list ) {
			synchronized( attribute_flake ) {
				attribute_list.removeIf(a -> a.getName().equals(attribute.getName()));
				AttributeUtility.setAttribute(attribute, attribute_flake);
			}
			attribute_list.add(attribute);
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public synchronized void delete() {
		if( is_deleted ) {
			return;
		}
		is_deleted = true;
		synchronized( attribute_list ) {
			synchronized( attribute_flake ) {
				attribute_flake.delete();
				attribute_list.clear();
			}
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void checkDeletion() {
		if( is_deleted ) {
			throw new StorageException("The AttributeCache has been deleted!");
		}
	}
	
	
	/**
	 * @return
	 */
	public long getAttributeFlakeIdentification() {
		return attribute_flake.getIdentification();
	}

	
	/* (non-Javadoc)
	 * @see j3l.util.check.IValidate#isValid()
	 */
	@Override public boolean isValid() {
		synchronized( attribute_flake ) {
			return !is_deleted && attribute_flake.isValid();
		}
	}
	
}
