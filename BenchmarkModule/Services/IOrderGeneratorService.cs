namespace BenchmarkModule.Services
{
    public interface IOrderGeneratorService
    {
        public Task GenerateOrders(int numberOfOrders);
    }
}
