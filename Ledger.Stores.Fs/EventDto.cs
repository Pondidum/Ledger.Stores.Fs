namespace Ledger.Stores.Fs
{
	public class EventDto<TKey>
	{
		public TKey ID { get; set; }
		public IDomainEvent<TKey> Event { get; set; }
	}
}
