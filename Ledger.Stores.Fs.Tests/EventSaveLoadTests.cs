﻿using System;
using System.IO;
using System.Linq;
using NSubstitute;
using Shouldly;
using TestsDomain.Events;
using Xunit;

namespace Ledger.Stores.Fs.Tests
{
	public class EventSaveLoadTests : IDisposable
	{
		private readonly string _root;

		public EventSaveLoadTests()
		{
			_root = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

			Directory.CreateDirectory(_root);
		}

		[Fact]
		public void The_events_should_keep_types()
		{
			var toSave = new DomainEvent[]
			{
				new NameChangedByDeedPoll {NewName = "Deed"},
				new FixNameSpelling {NewName = "Fix"},
			};

			var id = Guid.NewGuid();
			var store = new FileEventStore(_root);
			store.SaveEvents(id, toSave);

			var loaded = store.LoadEvents(id);

			loaded.First().ShouldBeOfType<NameChangedByDeedPoll>();
			loaded.Last().ShouldBeOfType<FixNameSpelling>();
		}

		[Fact]
		public void Only_events_for_the_correct_aggregate_are_returned()
		{
			var first = Guid.NewGuid();
			var second = Guid.NewGuid();

			var store = new FileEventStore(_root);
			store.SaveEvents(first, new[] { new FixNameSpelling { NewName = "Fix" } });
			store.SaveEvents(second, new[] { new NameChangedByDeedPoll { NewName = "Deed" } });

			var loaded = store.LoadEvents(first);

			loaded.Single().ShouldBeOfType<FixNameSpelling>();
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