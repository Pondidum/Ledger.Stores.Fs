using System;
using System.IO;
using Ledger.Acceptance;

namespace Ledger.Stores.Fs.Tests
{
	public class Acceptance : AcceptanceTests
	{
		public Acceptance() 
			: base(new FileEventStore(GetDirectory()))
		{
		}

		private static string GetDirectory()
		{
			var id = Guid.NewGuid().ToString();
			var temp = Path.GetTempPath();

			var path = Path.Combine(temp, id);

			Directory.CreateDirectory(path);

			return path;
		}

	}

}
