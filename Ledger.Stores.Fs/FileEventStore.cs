using System.IO;

namespace Ledger.Stores.Fs
{

	public class FileEventStore : IEventStore
	{
		private readonly IFileSystem _fileSystem;

		public string Directory { get; }

		public FileEventStore(string directory)
			: this(new PhysicalFileSystem(), directory)
		{
		}

		public FileEventStore(IFileSystem fs, string directory)
		{
			_fileSystem = fs;

			Directory = directory;
		}

		public IStoreReader<TKey> CreateReader<TKey>(EventStoreContext context)
		{
			return new FileStoreReader<TKey>(
				_fileSystem,
				EventFile(context.StreamName),
				SnapshotFile(context.StreamName));
		}

		public IStoreWriter<TKey> CreateWriter<TKey>(EventStoreContext context)
		{
			return new FileStoreWriter<TKey>(
				_fileSystem,
				EventFile(context.StreamName),
				SnapshotFile(context.StreamName));
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
