using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;
using System.Diagnostics;

namespace OpenFTTH.AddressPostgisProjector;

internal sealed class AddressPostgisProjectorHost : BackgroundService
{
    private readonly ILogger<AddressPostgisProjectorHost> _logger;
    private readonly IEventStore _eventStore;
    private readonly IPostgisAddressImport _postgisAddressImport;
    private const int _catchUpTimeMs = 600_000;

    public AddressPostgisProjectorHost(
        ILogger<AddressPostgisProjectorHost> logger,
        IEventStore eventStore,
        IPostgisAddressImport postgisAddressImport)
    {
        _logger = logger;
        _eventStore = eventStore;
        _postgisAddressImport = postgisAddressImport;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            _logger.LogInformation(
                "Starting {HostName}", nameof(AddressPostgisProjectorHost));

            _postgisAddressImport.Init();

            _logger.LogInformation("Starting dehydration.");
            await _eventStore
                .DehydrateProjectionsAsync(stoppingToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Memory after dehydration {MibiBytes}.",
                Process.GetCurrentProcess().PrivateMemorySize64 / 1024 / 1024);

            var projection = _eventStore.Projections.Get<AddressPostgisProjection>();

            _logger.LogInformation("Starting import.");
            await _postgisAddressImport.Import(projection).ConfigureAwait(false);

            _logger.LogInformation(
                "Memory after bulk write {MibiBytes}.",
                Process.GetCurrentProcess().PrivateMemorySize64 / 1024 / 1024);

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(_catchUpTimeMs, stoppingToken).ConfigureAwait(false);

                _logger.LogDebug("Checking for new events.");
                var changes = await _eventStore
                    .CatchUpAsync(stoppingToken)
                    .ConfigureAwait(false);

                if (changes > 0)
                {
                    _logger.LogInformation("{ChangeCount} changes, starting import.", changes);
                    await _postgisAddressImport.Import(projection).ConfigureAwait(false);
                    _logger.LogInformation("Finished changes importing {ChangeCount}.", changes);
                }
                else
                {
                    _logger.LogDebug("No changes since last run.");
                }
            }
        }
        catch (TaskCanceledException)
        {
            // Do nothing since this is valid.
            _logger.LogError("Cancellation requested.");
        }
    }
}
