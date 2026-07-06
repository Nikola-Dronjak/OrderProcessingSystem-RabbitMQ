using BenchmarkModule.DTOs;
using System.Net.Http.Json;

namespace BenchmarkModule.Services
{
    public class OrderGeneratorService : IOrderGeneratorService
    {
        private readonly HttpClient httpClient;
        private readonly IMetricsCollectorService metricsCollectorService;

        public OrderGeneratorService(HttpClient httpClient, IMetricsCollectorService metricsCollectorService)
        {
            this.httpClient = httpClient;
            this.metricsCollectorService = metricsCollectorService;
        }

        #region Public methods
        public async Task GenerateOrders(int numberOfOrders)
        {
            List<Task> tasks = new List<Task>(numberOfOrders);
            for (int i = 0; i < numberOfOrders; i++)
            {
                tasks.Add(this.CreateOrderAsync());
            }
            await Task.WhenAll(tasks);
        }
        #endregion

        #region Private methods
        private async Task CreateOrderAsync()
        {
            CreateOrderRequestDTO order = new CreateOrderRequestDTO
            {
                ProductId = "TestId",
                Quantity = 1,
                Price = 2.50m
            };

            HttpResponseMessage response = await this.httpClient.PostAsJsonAsync("/api/orders", order);
            response.EnsureSuccessStatusCode();

            CreateOrderResponseDTO? responseContent = await response.Content.ReadFromJsonAsync<CreateOrderResponseDTO>();
            if (responseContent == null)
            {
                throw new InvalidOperationException("Order service returned an invalid response.");
            }
            this.metricsCollectorService.RegisterOrder(responseContent.OrderId);
        }
        #endregion

    }
}
