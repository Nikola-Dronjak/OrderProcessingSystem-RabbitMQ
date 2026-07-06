using System.Collections.Concurrent;

namespace BenchmarkModule.Services
{
    public class MetricsCollectorService : IMetricsCollectorService
    {
        private readonly ILogger<MetricsCollectorService> logger;

        private readonly ConcurrentDictionary<Guid, DateTime> activeOrders = new ConcurrentDictionary<Guid, DateTime>();
        private readonly ConcurrentBag<double> latencies = new ConcurrentBag<double>();
        private readonly TaskCompletionSource benchmarkCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        private int numberOfExpectedOrders = 0;
        private int numberOfCompletedOrders = 0;
        private int numberOfSuccessfulOrders = 0;
        private int numberOfFailedOrders = 0;
        private DateTime benchmarkStartTime;
        private DateTime benchmarkEndTime;

        public MetricsCollectorService(ILogger<MetricsCollectorService> logger)
        {
            this.logger = logger;
        }

        public void StartBenchmark(int numberOfExpectedOrders)
        {
            this.numberOfExpectedOrders = numberOfExpectedOrders;
            benchmarkStartTime = DateTime.UtcNow;
        }

        public void RegisterOrder(Guid orderId)
        {
            this.activeOrders.TryAdd(orderId, DateTime.UtcNow);
        }

        public void RegisterOrderCompletion(Guid orderId, bool IsSuccessful)
        {
            if (!activeOrders.TryRemove(orderId, out DateTime start))
                return;

            double latency = (DateTime.UtcNow - start).TotalMilliseconds;
            this.latencies.Add(latency);

            Interlocked.Increment(ref numberOfCompletedOrders);
            if (IsSuccessful)
                Interlocked.Increment(ref numberOfSuccessfulOrders);
            else
                Interlocked.Increment(ref numberOfFailedOrders);

            // completion condition: BOTH success + failure count
            if (numberOfCompletedOrders == numberOfExpectedOrders)
            {
                benchmarkEndTime = DateTime.UtcNow;
                benchmarkCompleted.TrySetResult();
            }
        }

        public async Task WaitForCompletionAsync(CancellationToken cancellationToken)
        {
            await benchmarkCompleted.Task.WaitAsync(cancellationToken);
        }

        public void ShowBenchmarkResults()
        {
            TimeSpan totalTime = benchmarkEndTime - benchmarkStartTime;
            double throughput = numberOfCompletedOrders / totalTime.TotalSeconds;
            double averageLatency = latencies.Any() ? latencies.Average() : 0;
            double successRate = numberOfCompletedOrders == 0 ? 0 : (double)numberOfSuccessfulOrders / numberOfCompletedOrders * 100;
            this.logger.LogInformation("===== BENCHMARK RESULTS =====");
            this.logger.LogInformation($"Total completed orders: {numberOfCompletedOrders}");
            this.logger.LogInformation($"Number of successful orders: {numberOfSuccessfulOrders}");
            this.logger.LogInformation($"Number of failed orders: {numberOfFailedOrders}");
            this.logger.LogInformation($"Success rate: {successRate}");
            this.logger.LogInformation($"Total processing time: {totalTime.TotalSeconds}s");
            this.logger.LogInformation($"Average latency : {averageLatency:F2} ms");
            this.logger.LogInformation($"Throughput: {throughput:F2} orders/sec");
        }
    }
}
