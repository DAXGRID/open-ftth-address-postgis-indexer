using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace OpenFTTH.AddressPostgisProjector;

internal sealed class AddressPostgisProjectorHost : BackgroundService
{
    private readonly ILogger<AddressPostgisProjectorHost> _logger;

    public AddressPostgisProjectorHost(ILogger<AddressPostgisProjectorHost> logger)
    {
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting {HostName}", nameof(AddressPostgisProjectorHost));
        return Task.CompletedTask;
    }
}
