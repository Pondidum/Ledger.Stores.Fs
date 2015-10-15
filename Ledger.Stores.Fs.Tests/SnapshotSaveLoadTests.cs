using System;
using System.IO;
using Ledger.Acceptance.TestDomain;
using Ledger.Acceptance.TestObjects;
using Ledger.Conventions;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Fs.Tests
{
	public class SnapshotSaveLoadTests : IDisposable
	{
		private readonly string _root;
		private readonly FileEventStore<Guid> _store;
		private readonly StoreConventions _conventions;

		public SnapshotSaveLoadTests()
		{
			_root = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

			Directory.CreateDirectory(_root);
			_store = new FileEventStore<Guid>(_root);
			_conventions = new StoreConventions(new KeyTypeNamingConvention(), typeof(Guid), typeof(TestAggregate));
		}

		[Fact]
		public void A_snapshot_should_maintain_type()
		{
			var id = Guid.NewGuid();

			_store.SaveSnapshot(_conventions, id, new CandidateMemento());

			var loaded = _store.LoadLatestSnapshotFor(_conventions, id);

			loaded.ShouldBeOfType<CandidateMemento>();
		}

		[Fact]
		public void Only_the_latest_snapshot_should_be_loaded()
		{
			var id = Guid.NewGuid();

			_store.SaveSnapshot(_conventions, id, new CandidateMemento { Sequence = 4 });
			_store.SaveSnapshot(_conventions, id, new CandidateMemento { Sequence = 5 });

			_store
				.LoadLatestSnapshotFor(_conventions, id)
				.Sequence
				.ShouldBe(5);
		}

		[Fact]
		public void The_most_recent_snapshot_id_should_be_found()
		{
			var id = Guid.NewGuid();

			_store.SaveSnapshot(_conventions, id, new CandidateMemento { Sequence = 4 });
			_store.SaveSnapshot(_conventions, id, new CandidateMemento { Sequence = 5 });

			_store
				.GetLatestSnapshotSequenceFor(_conventions, id)
				.ShouldBe(5);
		}


		[Fact]
		public void When_there_is_no_snapshot_file_and_load_is_called()
		{
			var id = Guid.NewGuid();

			var loaded = _store.LoadLatestSnapshotFor(_conventions, id);

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
