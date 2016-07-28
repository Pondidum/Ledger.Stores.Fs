using System.Reflection;
using Ledger.Infrastructure;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Ledger.Stores.Fs
{
	internal class IgnoreStreamSequenceContractResolver : DefaultContractResolver
	{
		protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
		{
			var prop = base.CreateProperty(member, memberSerialization);

			if (prop.PropertyType == typeof(StreamSequence))
				prop.ShouldSerialize = instance => false;

			return prop;
		}
	}
}
