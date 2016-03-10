package snowflake.api;

import j3l.util.check.ArgumentChecker;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.03.10_0
 * @author Johannes B. Latzel
 */
public enum GlobalString {
	
	Flake("flake"),
	PositionInFlake("position_in_flake"),
	FlakeLockManager("flake_lock_manager"),
	Read("read"),
	Write("write"),
	FlakeStreamManager("flake_stream_manager"),
	ChunkMemory("chunk_memory"),
	StartAdress("start_address"),
	Length("length"),
	ChunkTableIndex("chunk_table_index"),
	IndexInFlake("index_in_flake"),
	ChunkData("chunk_data"),
	Buffer("buffer"),
	Chunk("chunk"),
	TableFile("table_file"),
	BinaryData("binary_data"),
	TableIndex("table_index"),
	FlakeDataManager("flake_data_manager"),
	Owner("owner"),
	ChunkManager("chunk_manager"),
	Index("index"),
	FlakeIdentificationStream("flake_identification_stream"),
	ClearChunk("clear_chunk"),
	CleaningTreshhold("cleaning_treshhold"),
	ChunkCollection("chunk_collection"),
	ChunkManagerConfiguration("chunk_manager_configuration"),
	StorageInformation("storage_information"),
	AllocateSpace("allocate_space"),
	FlakeModifier("flake_modifier"),
	ChunkTableFile("chunk_table_file"),
	NumberOfBytes("number_of_bytes"),
	Chunks("chunks"),
	Position("position"),
	Name("name"),
	StorageConfiguration("storage_configuration"),
	ConfigurationFile("configuration_file");
	
	
	private final String string;
	
	
	private GlobalString(String string) {
		this.string = ArgumentChecker.checkForEmptyString(string, "string");
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Enum#toString()
	 */
	@Override public String toString() {
		return string;
	}
	
}
