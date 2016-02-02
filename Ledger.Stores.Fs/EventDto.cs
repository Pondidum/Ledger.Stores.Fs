namespace Ledger.Stores.Fs
{
	public class EventDto<TKey>
	{
		public TKey ID { get; set; }
		public DomainEvent<TKey> Event { get; set; }
	}
}
