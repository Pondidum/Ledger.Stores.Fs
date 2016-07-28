using System.Collections.Generic;
using System.Linq;
using Ledger.Infrastructure;

namespace Ledger.Stores.Fs
{
	public class FileStoreReader<TKey> : FileStore, IStoreReader<TKey>
	{
		private readonly string _eventPath;
		private readonly string _snapshotPath;

		public FileStoreReader(IFileSystem fileSystem, string eventPath, string snapshotPath)
			: base(fileSystem)
		{
			_eventPath = eventPath;
			_snapshotPath = snapshotPath;
		}

		public IEnumerable<DomainEvent<TKey>> LoadEvents(TKey aggregateID)
		{
			return LoadEvents(_eventPath, aggregateID);
		}

		public IEnumerable<DomainEvent<TKey>> LoadEventsSince(TKey aggregateID, Sequence? sequence)
		{
			var events = LoadEvents(aggregateID);

			return sequence.HasValue
				? events.Where(e => e.Sequence > sequence)
				: events;
		}

		public Snapshot<TKey> LoadLatestSnapshotFor(TKey aggregateID)
		{
			return LoadLatestSnapshotFor(_snapshotPath, aggregateID);
		}

		public IEnumerable<TKey> LoadAllKeys()
		{
			return ReadFrom<EventDto<TKey>>(_eventPath)
				.Select(dto => dto.ID)
				.Distinct();
		}

		public IEnumerable<DomainEvent<TKey>> LoadAllEvents()
		{
			return ReadFrom<EventDto<TKey>>(_eventPath)
				.Apply((dto, i) => dto.Event.StreamSequence = new StreamSequence(i))
				.Select(dto => dto.Event);
		}


		public virtual void Dispose()
		{
		}
	}
}
