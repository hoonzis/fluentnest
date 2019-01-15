using System;
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

        private string AddSimpleTestData()
        {
            var indexName = "index_" + Guid.NewGuid();
            Client.CreateIndex(indexName, x => x.Mappings(m => m
            .Map<Car>(t => t
                .Properties(prop => prop.Keyword(str => str.Name(s => s.Guid).Index()))
                .Properties(prop => prop.Keyword(str => str.Name(s => s.Email).Index()))
            )));

            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Id = Guid.NewGuid(),
                    BIG_CASE_NAME = "big" + i % 3,
                    UPPERCASE_TIMESTAMP = new DateTime(2010, 1, 1)
                };

                Client.Index(car, ind => ind.Index(indexName));
            }
            Client.Flush(indexName);
            return indexName;
        }

        [Fact]
        public void TestCasing()
        {
            var indexName = AddSimpleTestData();
            var filter = Filters.CreateFilter<Car>(f => f.BIG_CASE_NAME == "big1");
            filter = filter.AndFilteredOn<Car>(f => f.BIG_CASE_NAME != "big2");
            var sc = new SearchDescriptor<Car>().Index(indexName).FilterOn(filter);
            var query = Serialize(sc);
            Check.That(query).Contains("BIG_CASE_NAME");
            var cars = Client.Search<Car>(sc).Hits.Select(h => h.Source);
            Check.That(cars).HasSize(3);
            Client.DeleteIndex(indexName);
        }

        [Fact]
        public void TestCasingComparison()
        {
            var indexName = AddSimpleTestData();
            var filter = Filters.CreateFilter<Car>(f => f.UPPERCASE_TIMESTAMP < new DateTime(2010, 1, 2));
            var sc = new SearchDescriptor<Car>().Index(indexName).FilterOn(filter);
            var query = Serialize(sc);
            Check.That(query).Contains("UPPERCASE_TIMESTAMP");
            var cars = Client.Search<Car>(sc).Hits.Select(h => h.Source);
            Check.That(cars).HasSize(10);
            Client.DeleteIndex(indexName);
        }
    }
}
