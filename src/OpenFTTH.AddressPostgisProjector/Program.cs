using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressPostgisProjector;

internal static class Program
{
    public static async Task Main()
    {
        using var host = HostConfig.Configure();
        host.Services.GetService<IEventStore>()!.ScanForProjections();
        await host.StartAsync().ConfigureAwait(false);
        await host.WaitForShutdownAsync().ConfigureAwait(false);
    }
}


