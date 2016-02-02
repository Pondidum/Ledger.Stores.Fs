using System;
using System.IO;
using System.Linq;
using Ledger.Acceptance;
using Ledger.Acceptance.TestDomain.Events;
using Ledger.Acceptance.TestObjects;
using Newtonsoft.Json;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Fs.Tests
{
	public class LoadAllTests
	{
		private static readonly EventStoreContext StreamName = new EventStoreContext("LoadStream", Default.SerializerSettings);

		private readonly string _root;
		private readonly FileEventStore _store;
		private readonly IncrementingStamper _stamper;

		public LoadAllTests()
		{
			_root = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

			Directory.CreateDirectory(_root);
			_store = new FileEventStore(_root);

			_stamper = new IncrementingStamper();
		}

		[Fact]
		public void When_loading_all_keys()
		{
			var firstID = Guid.NewGuid();
			var secondID = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(StreamName))
			{
				writer.SaveEvents(new[]
				{
					new TestEvent { AggregateID = firstID, Stamp = _stamper.GetNext() },
					new TestEvent { AggregateID = firstID, Stamp = _stamper.GetNext() },
					new TestEvent { AggregateID = secondID, Stamp = _stamper.GetNext() },
					new TestEvent { AggregateID = firstID, Stamp = _stamper.GetNext() },
					new TestEvent { AggregateID = secondID, Stamp = _stamper.GetNext() },
				});
			}

			using (var reader = _store.CreateReader<Guid>(StreamName))
			{
				reader.LoadAllKeys().ShouldBe(new[] { firstID, secondID }, true);
			}
		}


		[Fact]
		public void When_loading_all_events()
		{
			var firstID = Guid.NewGuid();
			var secondID = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(StreamName))
			{
				writer.SaveEvents(new DomainEvent<Guid>[]
				{
					new CandidateCreated { AggregateID = firstID, Stamp = _stamper.GetNext() },
					new AddEmailAddress { AggregateID = firstID, Stamp = _stamper.GetNext() },
					new FixNameSpelling  { AggregateID = secondID, Stamp = _stamper.GetNext() },
					new NameChangedByDeedPoll { AggregateID = firstID, Stamp = _stamper.GetNext() },
					new AddEmailAddress { AggregateID = secondID, Stamp = _stamper.GetNext() },
				});
			}

			using (var reader = _store.CreateReader<Guid>(StreamName))
			{
				reader.LoadAllEvents().Select(e => e.GetType()).ShouldBe(new[]
				{
					typeof(CandidateCreated),
					typeof(AddEmailAddress),
					typeof(FixNameSpelling),
					typeof(NameChangedByDeedPoll),
					typeof(AddEmailAddress)
				});
			}
		}
	}
}