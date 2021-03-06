﻿using System;
using System.IO;
using System.Linq;
using System.Text;
using Ledger.Acceptance;
using Ledger.Acceptance.TestDomain.Events;
using Ledger.Infrastructure;
using Newtonsoft.Json;
using NSubstitute;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Fs.Tests
{
	public class EventSaveLoadTests : IDisposable
	{
		private static readonly EventStoreContext StreamName =  new EventStoreContext("streamName", new DefaultTypeResolver());

		private readonly string _root;
		private readonly FileEventStore _store;

		public EventSaveLoadTests()
		{
			_root = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

			Directory.CreateDirectory(_root);
			_store = new FileEventStore(_root);
		}

		[Fact]
		public void The_events_should_keep_types()
		{
			var id = Guid.NewGuid();
			var toSave = new DomainEvent<Guid>[]
			{
				new NameChangedByDeedPoll {AggregateID = id, NewName = "Deed"},
				new FixNameSpelling {AggregateID = id, NewName = "Fix"},
			};

			_store.CreateWriter<Guid>(StreamName).SaveEvents(toSave);

			var loaded = _store.CreateReader<Guid>(StreamName).LoadEvents(id);

			loaded.First().ShouldBeOfType<NameChangedByDeedPoll>();
			loaded.Last().ShouldBeOfType<FixNameSpelling>();
		}

		[Fact]
		public void Only_events_for_the_correct_aggregate_are_returned()
		{
			var first = Guid.NewGuid();
			var second = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(StreamName))
			{
				writer.SaveEvents(new[] { new FixNameSpelling { AggregateID = first, NewName = "Fix" } });
				writer.SaveEvents(new[] { new NameChangedByDeedPoll { AggregateID = second, NewName = "Deed" } });
			}

			var loaded = _store.CreateReader<Guid>(StreamName).LoadEvents(first);

			loaded.Single().ShouldBeOfType<FixNameSpelling>();
		}

		[Fact]
		public void Only_the_latest_sequence_is_returned()
		{
			var first = Guid.NewGuid();
			var second = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(StreamName))
			{
				writer.SaveEvents(new[] { new FixNameSpelling { AggregateID = first, Sequence = 4.AsSequence()} });
				writer.SaveEvents(new[] { new FixNameSpelling { AggregateID = first, Sequence = 5.AsSequence() } });
				writer.SaveEvents(new[] { new NameChangedByDeedPoll {AggregateID = second, Sequence = 6.AsSequence() } });

				writer
					.GetLatestSequenceFor(first)
					.ShouldBe(5.AsSequence());
			}
		}

		[Fact]
		public void Loading_events_since_only_gets_events_after_the_sequence()
		{
			var id = Guid.NewGuid();

			var toSave = new DomainEvent<Guid>[]
			{
				new NameChangedByDeedPoll { AggregateID = id, Sequence = 3.AsSequence()},
				new FixNameSpelling { AggregateID = id, Sequence = 4.AsSequence() },
				new FixNameSpelling { AggregateID = id, Sequence = 5.AsSequence() },
				new FixNameSpelling { AggregateID = id, Sequence = 6.AsSequence() },
			};

			_store.CreateWriter<Guid>(StreamName).SaveEvents(toSave);

			var loaded = _store.CreateReader<Guid>(StreamName).LoadEventsSince(id, 4.AsSequence());

			loaded.Select(x => x.Sequence).ShouldBe(new[] { 5.AsSequence(), 6.AsSequence() });
		}

		[Fact]
		public void When_there_is_no_event_file_and_load_is_called()
		{
			var id = Guid.NewGuid();

			var loaded = _store.CreateReader<Guid>(StreamName).LoadEventsSince(id, 4.AsSequence());

			loaded.ShouldBeEmpty();
		}

		[Fact]
		public void When_writing_an_event_the_format_is_correct()
		{
			var stream = new MemoryStream();

			var fs = Substitute.For<IFileSystem>();
			fs.AppendTo(Arg.Any<string>()).Returns(stream);

			var store = new FileEventStore(fs, "store");

			var e = new FixNameSpelling { AggregateID = Guid.NewGuid(), NewName = "Name", Stamp = DateTime.Now, Sequence = new Sequence(25) };

			using (var writer = store.CreateWriter<Guid>(StreamName))
			{
				writer.SaveEvents(new [] { e });
			}

			var json = Encoding.UTF8.GetString(stream.ToArray()).Trim();

			json.ShouldNotContain("StreamSequence");

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
