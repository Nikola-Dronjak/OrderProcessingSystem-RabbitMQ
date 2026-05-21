using Common.Messaging.Extensions;
using OrderModule.Consumers;
using OrderModule.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddRabbitMQMessaging(builder.Configuration);

builder.Services.AddScoped<IOrderService, OrderService>();

builder.Services.AddHostedService<InventoryReservedConsumer>();

builder.Services.AddControllers();

var app = builder.Build();

// Configure the HTTP request pipeline.

app.MapControllers();

app.Run();
