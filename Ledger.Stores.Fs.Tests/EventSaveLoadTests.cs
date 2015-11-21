using System;
using System.IO;
using System.Linq;
using Ledger.Acceptance.TestDomain.Events;
using Ledger.Acceptance.TestObjects;
using Ledger.Conventions;
using NSubstitute;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Fs.Tests
{
	public class EventSaveLoadTests : IDisposable
	{
		private readonly string _root;
		private readonly FileEventStore _store;
		private readonly StoreConventions _conventions;

		public EventSaveLoadTests()
		{
			_root = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

			Directory.CreateDirectory(_root);
			_store = new FileEventStore(_root);
			_conventions = new StoreConventions(new KeyTypeNamingConvention(), typeof(Guid), typeof(TestAggregate));
		}

		[Fact]
		public void The_events_should_keep_types()
		{
			var toSave = new DomainEvent<Guid>[]
			{
				new NameChangedByDeedPoll {NewName = "Deed"},
				new FixNameSpelling {NewName = "Fix"},
			};

			var id = Guid.NewGuid();
			_store.CreateWriter<Guid>(_conventions).SaveEvents(id, toSave);

			var loaded = _store.CreateReader<Guid>(_conventions).LoadEvents(id);

			loaded.First().ShouldBeOfType<NameChangedByDeedPoll>();
			loaded.Last().ShouldBeOfType<FixNameSpelling>();
		}

		[Fact]
		public void Only_events_for_the_correct_aggregate_are_returned()
		{
			var first = Guid.NewGuid();
			var second = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(_conventions))
			{
				writer.SaveEvents(first, new[] { new FixNameSpelling { NewName = "Fix" } });
				writer.SaveEvents(second, new[] { new NameChangedByDeedPoll { NewName = "Deed" } });
			}

			var loaded = _store.CreateReader<Guid>(_conventions).LoadEvents(first);

			loaded.Single().ShouldBeOfType<FixNameSpelling>();
		}

		[Fact]
		public void Only_the_latest_sequence_is_returned()
		{
			var first = Guid.NewGuid();
			var second = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(_conventions))
			{
				writer.SaveEvents(first, new[] { new FixNameSpelling { Sequence = 4 } });
				writer.SaveEvents(first, new[] { new FixNameSpelling { Sequence = 5 } });
				writer.SaveEvents(second, new[] { new NameChangedByDeedPoll { Sequence = 6 } });

				writer
					.GetLatestSequenceFor(first)
					.ShouldBe(5);
			}
		}

		[Fact]
		public void Loading_events_since_only_gets_events_after_the_sequence()
		{
			var toSave = new DomainEvent<Guid>[]
			{
				new NameChangedByDeedPoll { Sequence = 3 },
				new FixNameSpelling { Sequence = 4 },
				new FixNameSpelling { Sequence = 5 },
				new FixNameSpelling { Sequence = 6 },
			};

			var id = Guid.NewGuid();

			_store.CreateWriter<Guid>(_conventions).SaveEvents(id, toSave);

			var loaded = _store.CreateReader<Guid>(_conventions).LoadEventsSince(id, 4);

			loaded.Select(x => x.Sequence).ShouldBe(new[] { 5, 6 });
		}

		[Fact]
		public void When_there_is_no_event_file_and_load_is_called()
		{
			var id = Guid.NewGuid();

			var loaded = _store.CreateReader<Guid>(_conventions).LoadEventsSince(id, 4);

			loaded.ShouldBeEmpty();
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
