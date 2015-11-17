using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Ledger.Infrastructure;
using Newtonsoft.Json;

namespace Ledger.Stores.Fs
{
	public class FileStoreReaderWriter<TKey> : IStoreReader<TKey>, IStoreWriter<TKey>
	{
		private readonly IFileSystem _fileSystem;
		private readonly JsonSerializerSettings _jsonSettings;
		private readonly string _eventPath;
		private readonly string _snapshotPath;

		public FileStoreReaderWriter(IFileSystem fileSystem, JsonSerializerSettings jsonSettings, string eventPath, string snapshotPath)
		{
			_fileSystem = fileSystem;
			_jsonSettings = jsonSettings;
			_eventPath = eventPath;
			_snapshotPath = snapshotPath;
		}

		public IEnumerable<IDomainEvent> LoadEvents(TKey aggregateID)
		{
			return ReadFrom<EventDto<TKey>>(_eventPath)
				.Where(dto => Equals(dto.ID, aggregateID))
				.Select(dto => dto.Event);
		}

		public IEnumerable<IDomainEvent> LoadEventsSince(TKey aggregateID, int sequenceID)
		{
			return LoadEvents(aggregateID)
				.Where(e => e.Sequence > sequenceID);
		}

		public ISequenced LoadLatestSnapshotFor(TKey aggregateID)
		{
			return ReadFrom<SnapshotDto<TKey>>(_snapshotPath)
				.Where(dto => Equals(dto.ID, aggregateID))
				.Select(dto => dto.Snapshot)
				.Cast<ISequenced>()
				.LastOrDefault();
		}

		private IEnumerable<TDto> ReadFrom<TDto>(string filepath)
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
					yield return JsonConvert.DeserializeObject<TDto>(line, _jsonSettings);
				}
			}
		}

		private void AppendTo(string filepath, Action<StreamWriter> action)
		{
			using (var fs = _fileSystem.AppendTo(filepath))
			using (var sw = new StreamWriter(fs))
			{
				action(sw);
			}
		}

		public void SaveEvents(TKey aggregateID, IEnumerable<IDomainEvent> changes)
		{
			AppendTo(_eventPath, file =>
			{
				changes.ForEach(change =>
				{
					var dto = new EventDto<TKey> {ID = aggregateID, Event = change};
					var json = JsonConvert.SerializeObject(dto, _jsonSettings);

					file.WriteLine(json);
				});
			});
		}

		public void SaveSnapshot(TKey aggregateID, ISequenced snapshot)
		{
			AppendTo(_snapshotPath, file =>
			{
				var dto = new SnapshotDto<TKey> {ID = aggregateID, Snapshot = snapshot};
				var json = JsonConvert.SerializeObject(dto, _jsonSettings);

				file.WriteLine(json);
			});
		}

		public int? GetLatestSequenceFor(TKey aggregateID)
		{
			return LoadEvents(aggregateID)
				.Select(e => (int?) e.Sequence)
				.Max();
		}

		public int? GetLatestSnapshotSequenceFor(TKey aggregateID)
		{
			var snapshot = LoadLatestSnapshotFor(aggregateID);

			return snapshot != null
				? snapshot.Sequence
				: (int?) null;
		}

		public virtual void Dispose()
		{
		}
	}
}
