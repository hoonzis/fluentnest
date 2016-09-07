using System.Linq;
using FluentNest.Tests.Model;
using Nest;
using NFluent;
using Tests;
using Xunit;

namespace FluentNest.Tests
{
    public class PropertyNamesTests : TestsBase
    {
        public PropertyNamesTests()
            : base(null, connectionSettings => connectionSettings.DefaultFieldNameInferrer(p => p).DefaultTypeNameInferrer(p => p.Name))
        {

        }

        private void AddSimpleTestData()
        {
            Client.DeleteIndex(CarIndex);
            Client.CreateIndex(CarIndex, x => x.Mappings(m => m
            .Map<Car>(t => t
                .Properties(prop => prop.String(str => str.Name(s => s.Guid).Index(FieldIndexOption.NotAnalyzed)))
                .Properties(prop => prop.String(str => str.Name(s => s.Email).Index(FieldIndexOption.NotAnalyzed)))
            )));

            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    BIG_CASE_NAME = "big" + i % 3
                };

                Client.Index(car, ind => ind.Index(CarIndex));
            }
            Client.Flush(CarIndex);
        }

        [Fact]
        public void TestCasing()
        {
            AddSimpleTestData();
            var filter = Filters.CreateFilter<Car>(f => f.BIG_CASE_NAME == "big1");
            filter = filter.AndFilteredOn<Car>(f => f.BIG_CASE_NAME != "big2");
            var sc = new SearchDescriptor<Car>().FilterOn(filter);           
            var query = Serialize(sc);
            Check.That(query).Contains("BIG_CASE_NAME");
            var cars = Client.Search<Car>(sc).Hits.Select(h => h.Source);
            Check.That(cars).HasSize(3);
        }
    }
}
