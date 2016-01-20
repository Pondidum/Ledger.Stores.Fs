using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Ledger.Infrastructure;
using Newtonsoft.Json;

namespace Ledger.Stores.Fs
{
	public class FileStoreWriter<TKey> : FileStore, IStoreWriter<TKey>
	{
		private readonly string _eventPath;
		private readonly string _snapshotPath;

		public FileStoreWriter(IFileSystem fileSystem, JsonSerializerSettings jsonSettings, string eventPath, string snapshotPath)
			: base(fileSystem, jsonSettings)
		{
			_eventPath = eventPath;
			_snapshotPath = snapshotPath;
		}

		public int GetNumberOfEventsSinceSnapshotFor(TKey aggregateID)
		{
			var last = LoadLatestSnapshotFor(_snapshotPath, aggregateID);
			var stamp = last?.Stamp ?? DateTime.MinValue;

			return LoadEvents(_eventPath, aggregateID)
				.Count(e => e.Stamp > stamp);
		}

		public void SaveEvents(IEnumerable<IDomainEvent<TKey>> changes)
		{
			AppendTo(_eventPath, changes.Select(change => new EventDto<TKey> { ID = change.AggregateID, Event = change }));
		}

		public void SaveSnapshot(ISnapshot<TKey> snapshot)
		{
			AppendTo(_snapshotPath, new[] { new SnapshotDto<TKey> { ID = snapshot.AggregateID, Snapshot = snapshot } });
		}

		public DateTime? GetLatestStampFor(TKey aggregateID)
		{
			return LoadEvents(_eventPath, aggregateID)
				.Select(e => (DateTime?)e.Stamp)
				.Max();
		}

		public virtual void Dispose()
		{
		}
	}

}
