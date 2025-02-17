using Soenneker.Tests.FixturedUnit;
using Xunit;

namespace Soenneker.Zelos.Container.Tests;

[Collection("Collection")]
public class ZelosContainerTests : FixturedUnitTest
{
    public ZelosContainerTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {

    }

    [Fact]
    public void Default()
    {

    }
}
