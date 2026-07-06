namespace BenchmarkModule.Services
{
    public interface IMetricsCollectorService
    {
        public void StartBenchmark(int expectedOrders);

        public void RegisterOrder(Guid orderId);

        public void RegisterOrderCompletion(Guid orderId, bool isSuccessful);

        public Task WaitForCompletionAsync(CancellationToken cancellationToken);

        public void ShowBenchmarkResults();
    }
}
