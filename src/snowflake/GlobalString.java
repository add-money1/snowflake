package snowflake;

import j3l.util.check.ArgumentChecker;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.08_0
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
	StartAddress("start_address"),
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
	ConfigurationFile("configuration_file"),
	StreamDescription("stream_description"),
	DataFile("data_file"), 
	ChannelManager("channel_manager"),
	NumberOfChannel("number_of_channel"),
	Channel("channel"),
	ChannelManagerConfiguration("channel_manager_configuration"),
	ChannelReturner("channel_returner"),
	Directory("directory"),
	ParentDirectory("parent_directory"),
	Node("node"),
	AttributeValue("attribute_value"),
	TypeOffset("type_offset"),
	ValueOffset("value_offset"),
	LastChangedTimeStamp("last_changed_time_stamp"),
	Attribute("attribute"),
	BufferLength("buffer_length"),
	FlakeInputStream("flake_input_stream"),
	AttributeHeader("attribute_header"),
	ConstructorFunction("constructor_function"),
	AttributeFlake("attribute_flake"),
	DataFlake("data_flake"),
	AttributeHeaderIndex("attribute_header_index"),
	ValueLength("value_length"),
	Storage("storage"),
	Path("path"),
	TableFlake("table_flake"),
	File("file"),
	DataEntrySize("data_entry_size"),
	FileSystem("file_system"),
	BlockSize("block_size"),
	Identification("identification"),
	DeduplicationBlock("deduplication_block"),
	DataBlock("data_block"),
	DeduplicationTable("deduplication_table"),
	DeduplicationTableFlake("deduplication_table_flake"),
	LeftChunk("left_chunk"),
	RightChunk("right_chunk"),
	AvailableIndexList("available_index_list"),
	ChunkList("chunk_list");
	
	
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
