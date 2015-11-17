using System;
using StructureMap.Configuration.DSL;

namespace Ledger.Stores.Fs.Tests
{
	public class AcceptanceRegistry : Registry
	{
		public AcceptanceRegistry()
		{
			For<IEventStore>().Use<WrappedStore>();
		}
	}

	public class WrappedStore : FileEventStore
	{
		public WrappedStore()
			: base(Guid.NewGuid().ToString())
		{
			System.IO.Directory.CreateDirectory(Directory);
		}
	}
}
