using System;
using System.IO;
using Ledger.Acceptance;
using Ledger.Acceptance.TestDomain;
using Newtonsoft.Json;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Fs.Tests
{
	public class SnapshotSaveLoadTests : IDisposable
	{
		private static readonly EventStoreContext StreamName = new EventStoreContext("streamName", new DefaultTypeResolver());

		private readonly string _root;
		private readonly FileEventStore _store;
		private readonly IncrementingStamper _stamper;

		public SnapshotSaveLoadTests()
		{
			_root = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

			Directory.CreateDirectory(_root);
			_store = new FileEventStore(_root);

			_stamper = new IncrementingStamper();
		}

		[Fact]
		public void A_snapshot_should_maintain_type()
		{
			var id = Guid.NewGuid();

			_store.CreateWriter<Guid>(StreamName).SaveSnapshot(new CandidateMemento { AggregateID = id });

			var loaded = _store.CreateReader<Guid>(StreamName).LoadLatestSnapshotFor(id);

			loaded.ShouldBeOfType<CandidateMemento>();
		}

		[Fact]
		public void Only_the_latest_snapshot_should_be_loaded()
		{
			var id = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(StreamName))
			{
				writer.SaveSnapshot(new CandidateMemento { AggregateID = id, Stamp = _stamper.Offset(4) });
				writer.SaveSnapshot(new CandidateMemento { AggregateID = id, Stamp = _stamper.Offset(5) });
			}

			_store
				.CreateReader<Guid>(StreamName)
				.LoadLatestSnapshotFor(id)
				.Stamp
				.ShouldBe(_stamper.Offset(5));
		}

		[Fact]
		public void When_there_is_no_snapshot_file_and_load_is_called()
		{
			var id = Guid.NewGuid();

			var loaded = _store.CreateReader<Guid>(StreamName).LoadLatestSnapshotFor(id);

			loaded.ShouldBe(null);
		}

		public void Dispose()
		{
			try
			{
				Directory.Delete(_root, true);
			}
			catch (Exception)
			{
			}
		}
	}
}
