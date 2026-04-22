using Soenneker.Tests.HostedUnit;

namespace Soenneker.Zelos.Container.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public class ZelosContainerTests : HostedUnitTest
{
    public ZelosContainerTests(Host host) : base(host)
    {

    }

    [Test]
    public void Default()
    {

    }
}
