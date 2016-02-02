using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Ledger.Infrastructure;
using Newtonsoft.Json;

namespace Ledger.Stores.Fs
{
	public class FileStore
	{
		private readonly IFileSystem _fileSystem;
		private readonly JsonSerializerSettings _jsonSettings;

		public FileStore(IFileSystem fileSystem, JsonSerializerSettings jsonSettings)
		{
			_fileSystem = fileSystem;
			_jsonSettings = jsonSettings;
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
					yield return JsonConvert.DeserializeObject<TDto>(line, _jsonSettings);
				}
			}
		}

		protected void AppendTo(string filepath, IEnumerable<object> changes)
		{
			using (var fs = _fileSystem.AppendTo(filepath))
			using (var sw = new StreamWriter(fs))
			{
				changes
					.Select(change => JsonConvert.SerializeObject(change, _jsonSettings))
					.ForEach(json => sw.WriteLine(json));
			}
		}

		protected IEnumerable<DomainEvent<TKey>> LoadEvents<TKey>(string eventPath, TKey aggregateID)
		{
			return ReadFrom<EventDto<TKey>>(eventPath)
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