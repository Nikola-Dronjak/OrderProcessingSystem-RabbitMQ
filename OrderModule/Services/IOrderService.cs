using OrderModule.Models;

namespace OrderModule.Services
{
    public interface IOrderService
    {
        Task<Guid> CreateOrderAsync(CreateOrderRequest createOrderRequest, CancellationToken cancellationToken);
    }
}
