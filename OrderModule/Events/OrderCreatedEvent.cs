namespace OrderModule.Events
{
    public class OrderCreatedEvent
    {
        public Guid OrderId { get; set; }
        public string ProductId { get; set; }
        public int Quantity { get; set; }
        public decimal Price { get; set; }
        public DateTime CreatedDate { get; set; }
        public string CorrelationId { get; set; }
    }
}
