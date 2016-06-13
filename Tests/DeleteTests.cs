using FluentNest;
using NFluent;
using TestModel;
using Xunit;

namespace Tests
{
    public class DeleteTests : TestsBase
    {
        [Fact]
        public void DeleteByQuery()
        {
            AddSimpleTestData();
            client.DeleteByQuery<Car>(s => s.FilteredOn(x => x.Sold == true));
            var result = client.Search<Car>(sc => sc.MatchAll());
            Check.That(result.Hits).HasSize(5);
        }

        [Fact]
        public void DeleteByQuery_FilterCreatedSeparately()
        {
            AddSimpleTestData();
            var filter = Filters.CreateFilter<Car>(x => x.EngineType == EngineType.Diesel);
            client.DeleteByQuery<Car>(s => s.FilteredOn(filter));
            var result = client.Search<Car>(sc => sc.MatchAll());
            Check.That(result.Hits).HasSize(5);
        }
    }
}
