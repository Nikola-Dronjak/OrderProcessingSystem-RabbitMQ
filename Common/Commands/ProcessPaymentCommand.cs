namespace Common.Commands
{
    public class ProcessPaymentCommand : BaseCommand
    {
        public Guid OrderId { get; set; }
        public decimal Price { get; set; }
    }
}
