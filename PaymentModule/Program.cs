using Common.Messaging.Extensions;
using PaymentModule.Consumers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddRabbitMQMessaging(builder.Configuration);

builder.Services.AddHostedService<ProcessPaymentConsumer>();

var host = builder.Build();
host.Run();
