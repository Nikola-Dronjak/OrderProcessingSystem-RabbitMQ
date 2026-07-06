namespace BenchmarkModule.DTOs
{
    public class CreateOrderRequestDTO
    {
        public string ProductId { get; set; }
        public int Quantity { get; set; }
        public decimal Price { get; set; }
    }
}
