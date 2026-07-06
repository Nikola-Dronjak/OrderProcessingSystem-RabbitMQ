using BenchmarkModule.Services;

namespace BenchmarkModule
{
    public class BenchmarkWorker : BackgroundService
    {
        private readonly IOrderGeneratorService orderGeneratorService;
        private readonly IMetricsCollectorService metricsCollectorService;
        private readonly ILogger<BenchmarkWorker> logger;

        public BenchmarkWorker(IOrderGeneratorService orderGeneratorService, IMetricsCollectorService metricsCollectorService, ILogger<BenchmarkWorker> logger)
        {
            this.orderGeneratorService = orderGeneratorService;
            this.metricsCollectorService = metricsCollectorService;
            this.logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            this.logger.LogInformation("Starting benchmark...");
            this.metricsCollectorService.StartBenchmark(100);
            await this.orderGeneratorService.GenerateOrders(100);
            this.logger.LogInformation("Waiting for benchmark to complete...");
            await this.metricsCollectorService.WaitForCompletionAsync(stoppingToken);
            this.metricsCollectorService.ShowBenchmarkResults();

            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
    }
}
