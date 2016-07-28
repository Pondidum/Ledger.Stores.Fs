using System.Collections.Generic;
using System.Linq;
using Ledger.Infrastructure;

namespace Ledger.Stores.Fs
{
	public class FileStoreWriter<TKey> : FileStore, IStoreWriter<TKey>
	{
		private readonly string _eventPath;
		private readonly string _snapshotPath;

		public FileStoreWriter(IFileSystem fileSystem, string eventPath, string snapshotPath)
			: base(fileSystem)
		{
			_eventPath = eventPath;
			_snapshotPath = snapshotPath;
		}

		public int GetNumberOfEventsSinceSnapshotFor(TKey aggregateID)
		{
			var last = LoadLatestSnapshotFor(_snapshotPath, aggregateID);
			var sequence = last?.Sequence ?? Sequence.Start;

			return LoadEvents(_eventPath, aggregateID)
				.Count(e => e.Sequence > sequence);
		}

		public void SaveEvents(IEnumerable<DomainEvent<TKey>> changes)
		{
			AppendTo(_eventPath, changes.Select(change => new EventDto<TKey> { ID = change.AggregateID, Event = change }));
		}

		public void SaveSnapshot(Snapshot<TKey> snapshot)
		{
			AppendTo(_snapshotPath, new[] { new SnapshotDto<TKey> { ID = snapshot.AggregateID, Snapshot = snapshot } });
		}

		public Sequence? GetLatestSequenceFor(TKey aggregateID)
		{
			return LoadEvents(_eventPath, aggregateID)
				.Select(e => e.Sequence)
				.Max();
		}

		public virtual void Dispose()
		{
		}
	}

}
