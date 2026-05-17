using Microsoft.AspNetCore.Mvc;
using OrderModule.Models;
using OrderModule.Services;

namespace OrderModule.Controllers
{
    [ApiController]
    [Route("api/orders")]
    public class OrderController : ControllerBase
    {
        private readonly IOrderService orderService;

        public OrderController(IOrderService orderService)
        {
            this.orderService = orderService;
        }

        [HttpPost]
        public async Task<IActionResult> CreateOrder([FromBody] CreateOrderRequest request, CancellationToken cancellationToken)
        {
            Guid orderId = await this.orderService.CreateOrderAsync(request, cancellationToken);
            return Accepted(new
            {
                OrderId = orderId,
                Message = "Order creation initiated. You will receive a notification once the order is processed."
            });
        }
    }
}
