namespace OrderModule.Models
{
    public class CreateOrderRequest
    {
        public string ProductId { get; set; }
        public int Quantity { get; set; }
        public decimal Price { get; set; }
    }
}
