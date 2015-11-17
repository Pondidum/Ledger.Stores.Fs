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


		public IStoreReader<TKey> CreateReader<TKey>(IStoreConventions storeConventions)
		{
			return new FileStoreReaderWriter<TKey>(
				_fileSystem,
				_jsonSettings,
				EventFile(storeConventions),
				SnapshotFile(storeConventions));
		}

		public IStoreWriter<TKey> CreateWriter<TKey>(IStoreConventions storeConventions)
		{
			return new FileStoreReaderWriter<TKey>(
				_fileSystem,
				_jsonSettings,
				EventFile(storeConventions),
				SnapshotFile(storeConventions));
		}

		private string EventFile(IStoreConventions conventions)
		{
			return Path.Combine(Directory, conventions.EventStoreName() + ".json");
		}

		private string SnapshotFile(IStoreConventions conventions)
		{
			return Path.Combine(Directory, conventions.SnapshotStoreName() + ".json");
		}

	}
}
