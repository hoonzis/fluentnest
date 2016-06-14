using FluentNest.Tests.Model;
using NFluent;
using Xunit;

namespace FluentNest.Tests
{
    public class DeleteTests : TestsBase
    {
        [Fact]
        public void DeleteByQuery()
        {
            AddSimpleTestData();
            Client.DeleteByQuery<Car>(s => s.FilteredOn(x => x.Sold == true));
            var result = Client.Search<Car>(sc => sc.MatchAll());
            Check.That(result.Hits).HasSize(5);
        }

        [Fact]
        public void DeleteByQuery_FilterCreatedSeparately()
        {
            AddSimpleTestData();
            var filter = Filters.CreateFilter<Car>(x => x.EngineType == EngineType.Diesel);
            Client.DeleteByQuery<Car>(s => s.FilteredOn(filter));
            var result = Client.Search<Car>(sc => sc.MatchAll());
            Check.That(result.Hits).HasSize(5);
        }
    }
}
