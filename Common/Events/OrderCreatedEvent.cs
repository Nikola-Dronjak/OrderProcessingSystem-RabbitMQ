namespace Common.Events
{
    public class OrderCreatedEvent : BaseEvent
    {
        public Guid OrderId { get; set; }
        public string ProductId { get; set; }
        public int Quantity { get; set; }
        public decimal Price { get; set; }
    }
}
