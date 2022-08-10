using Microsoft.Extensions.Hosting;

namespace OpenFTTH.AddressPostgisProjector;

internal static class Program
{
    public static async Task Main()
    {
        using var host = HostConfig.Configure();
        await host.StartAsync().ConfigureAwait(false);
        await host.WaitForShutdownAsync().ConfigureAwait(false);
    }
}


