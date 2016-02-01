using FluentNest;
using Nest;
using NFluent;
using static Nest.Infer;
using TestModel;
using Xunit;
using Indices = Nest.Indices;

namespace Tests
{
    public class DeleteTests : TestsBase
    {
        [Fact]
        public void DeleteByQuery()
        {
            AddSimpleTestData();
            var response = client.DeleteByQuery<Car>(Index<Car>(), s => s.FilteredOn(x => x.Sold == true));
            var result = client.Search<Car>(sc => sc.MatchAll());
            Check.That(result.Hits).HasSize(5);
        }

        [Fact]
        public void DeleteByQuery_FilterCreatedSeparately()
        {
            AddSimpleTestData();
            var filter = NestHelperMethods.CreateFilter<Car>(x => x.EngineType == EngineType.Diesel);
            client.DeleteByQuery<Car>(Indices<Car>(), s => s.FilteredOn(filter));
            var result = client.Search<Car>(sc => sc.MatchAll());
            Check.That(result.Hits).HasSize(5);
        }
    }
}
