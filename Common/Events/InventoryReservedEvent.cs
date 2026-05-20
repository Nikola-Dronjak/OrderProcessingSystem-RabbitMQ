namespace Common.Events
{
    public class InventoryReservedEvent : BaseEvent
    {
        public Guid OrderId { get; set; }
        public string ProductId { get; set; }
        public int Quantity { get; set; }
    }
}
