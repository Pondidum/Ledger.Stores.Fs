using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace Ledger.Stores.Fs
{
	public class FileStoreReader<TKey> : FileStore, IStoreReader<TKey>
	{
		private readonly string _eventPath;
		private readonly string _snapshotPath;

		public FileStoreReader(IFileSystem fileSystem, JsonSerializerSettings jsonSettings, string eventPath, string snapshotPath)
			: base(fileSystem, jsonSettings)
		{
			_eventPath = eventPath;
			_snapshotPath = snapshotPath;
		}

		public IEnumerable<IDomainEvent<TKey>> LoadEvents(TKey aggregateID)
		{
			return LoadEvents(_eventPath, aggregateID);
		}

		public IEnumerable<IDomainEvent<TKey>> LoadEventsSince(TKey aggregateID, DateTime stamp)
		{
			return LoadEvents(aggregateID)
				.Where(e => e.Stamp > stamp);
		}

		public ISnapshot<TKey> LoadLatestSnapshotFor(TKey aggregateID)
		{
			return LoadLatestSnapshotFor(_snapshotPath, aggregateID);
		}

		public IEnumerable<TKey> LoadAllKeys()
		{
			return ReadFrom<EventDto<TKey>>(_eventPath)
				.Select(dto => dto.ID)
				.Distinct();
		}

		public IEnumerable<IDomainEvent<TKey>> LoadAllEvents()
		{
			return ReadFrom<EventDto<TKey>>(_eventPath)
				.Select(dto => dto.Event);
		}


		public virtual void Dispose()
		{
		}
	}
}
