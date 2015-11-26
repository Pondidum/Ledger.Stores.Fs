using System.IO;
using Newtonsoft.Json;

namespace Ledger.Stores.Fs
{

	public class FileEventStore : IEventStore
	{
		private readonly IFileSystem _fileSystem;
		private readonly JsonSerializerSettings _jsonSettings;

		public string Directory { get; }

		public FileEventStore(string directory)
			: this(new PhysicalFileSystem(), directory)
		{
		}

		public FileEventStore(IFileSystem fs, string directory)
		{
			_fileSystem = fs;
			_jsonSettings = new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.Auto};

			Directory = directory;
		}


		public IStoreReader<TKey> CreateReader<TKey>(string stream)
		{
			return new FileStoreReaderWriter<TKey>(
				_fileSystem,
				_jsonSettings,
				EventFile(stream),
				SnapshotFile(stream));
		}

		public IStoreWriter<TKey> CreateWriter<TKey>(string stream)
		{
			return new FileStoreReaderWriter<TKey>(
				_fileSystem,
				_jsonSettings,
				EventFile(stream),
				SnapshotFile(stream));
		}

		private string EventFile(string stream)
		{
			return Path.Combine(Directory, stream + ".events.json");
		}

		private string SnapshotFile(string stream)
		{
			return Path.Combine(Directory, stream + ".snapshots.json");
		}

	}
}
