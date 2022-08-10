using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;
using System.Diagnostics;

namespace OpenFTTH.AddressPostgisProjector;

internal sealed class AddressPostgisProjectorHost : BackgroundService
{
    private readonly ILogger<AddressPostgisProjectorHost> _logger;
    private readonly IEventStore _eventStore;
    private const int _catchUpTimeMs = 60000;

    public AddressPostgisProjectorHost(
        ILogger<AddressPostgisProjectorHost> logger,
        IEventStore eventStore)
    {
        _logger = logger;
        _eventStore = eventStore;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            _logger.LogInformation(
                "Starting {HostName}", nameof(AddressPostgisProjectorHost));

            _logger.LogInformation("Starting dehydration.");
            await _eventStore
                .DehydrateProjectionsAsync(stoppingToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Memory after dehydration {MibiBytes}.",
                Process.GetCurrentProcess().PrivateMemorySize64 / 1024 / 1024);

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(_catchUpTimeMs, stoppingToken).ConfigureAwait(false);

                _logger.LogInformation("Checking for new events.");
                var changes = await _eventStore
                    .CatchUpAsync(stoppingToken)
                    .ConfigureAwait(false);

                if (changes > 0)
                {
                    _logger.LogInformation("{Count} changes so we do import.", changes);
                }
                else
                {
                    _logger.LogInformation("No changes since last run.");
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError("{Exception}", ex);
            throw;
        }
    }
}
