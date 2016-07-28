using System.Collections.Generic;
using System.IO;
using System.Linq;
using Ledger.Infrastructure;

namespace Ledger.Stores.Fs
{
	public class FileStore
	{
		private readonly IFileSystem _fileSystem;

		public FileStore(IFileSystem fileSystem)
		{
			_fileSystem = fileSystem;
		}

		protected IEnumerable<TDto> ReadFrom<TDto>(string filepath)
		{
			if (_fileSystem.FileExists(filepath) == false)
			{
				return Enumerable.Empty<TDto>();
			}

			return ReadFromImpl<TDto>(filepath);
		}

		private IEnumerable<TDto> ReadFromImpl<TDto>(string filepath)
		{
			using (var fs = _fileSystem.ReadFile(filepath))
			using (var sr = new StreamReader(fs))
			{
				string line;
				while ((line = sr.ReadLine()) != null)
				{
					yield return Serializer.Deserialize<TDto>(line);
				}
			}
		}

		protected void AppendTo(string filepath, IEnumerable<object> changes)
		{
			using (var fs = _fileSystem.AppendTo(filepath))
			using (var sw = new StreamWriter(fs))
			{
				changes
					.Select(change => Serializer.Serialize(change))
					.ForEach(json => sw.WriteLine(json));
			}
		}

		protected IEnumerable<DomainEvent<TKey>> LoadEvents<TKey>(string eventPath, TKey aggregateID)
		{
			return ReadFrom<EventDto<TKey>>(eventPath)
				.Apply((dto, i) => dto.Event.StreamSequence = new StreamSequence(i))
				.Where(dto => Equals(dto.ID, aggregateID))
				.Select(dto => dto.Event);
		}

		protected Snapshot<TKey> LoadLatestSnapshotFor<TKey>(string snapshotPath, TKey aggregateID)
		{
			return ReadFrom<SnapshotDto<TKey>>(snapshotPath)
				.Where(dto => Equals(dto.ID, aggregateID))
				.Select(dto => dto.Snapshot)
				.Cast<Snapshot<TKey>>()
				.LastOrDefault();
		}
	}
}