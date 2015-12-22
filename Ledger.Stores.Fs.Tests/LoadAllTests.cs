using System;
using System.IO;
using Ledger.Acceptance;
using Ledger.Acceptance.TestObjects;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Fs.Tests
{
	public class LoadAllTests
	{
		private const string StreamName = "LoadStream";

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
	}
}