namespace Common.Events
{
    public class OrderCompletedEvent : BaseEvent
    {
        public Guid OrderId { get; set; }
        public decimal Price { get; set; }
        public Guid PaymentId { get; set; }
    }
}
