namespace Common.Events
{
    public class PaymentSucceededEvent : BaseEvent
    {
        public Guid OrderId { get; set; }
        public decimal Price { get; set; }
        public Guid PaymentId { get; set; }
    }
}
