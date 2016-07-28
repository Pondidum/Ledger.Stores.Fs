using System;
using Newtonsoft.Json;

namespace Ledger.Stores.Fs
{
	public static class Serializer
	{
		private static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
		{
			TypeNameHandling = TypeNameHandling.Auto,
			Converters = new JsonConverter[] { new SequenceJsonConverter() }
		};

		public static string Serialize(object value)
		{
			return JsonConvert.SerializeObject(value, Settings);
		}

		public static object Deserialize(string json, Type type)
		{
			return JsonConvert.DeserializeObject(json, type, Settings);
		}
	}
}
