using System;
using FluentNest.Tests.Model;
using Nest;
using NFluent;
using Tests;
using Xunit;

namespace FluentNest.Tests
{
    public class DeleteTests : TestsBase
    {
        public string AddSimpleTestData()
        {
            var indexName = "index_" + Guid.NewGuid();
            Client.CreateIndex(indexName, x => x.Mappings(
                m => m.Map<Car>(t => t
            .Properties(prop => prop.String(str => str.Name(s => s.EngineType).Index(FieldIndexOption.NotAnalyzed))))));

            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Timestamp = new DateTime(2010, i + 1, 1),
                    Sold = i % 2 == 0,
                    CarType = "Type" + i % 3,
                    Length = i,
                    EngineType = i % 2 == 0 ? EngineType.Diesel : EngineType.Standard,
                    Weight = 5
                };

                Client.Index(car, ind => ind.Index(indexName));
            }
            Client.Flush(indexName);
            return indexName;
        }

        [Fact]
        public void DeleteByQuery()
        {
            var index = AddSimpleTestData();
            var deleteResult = Client.DeleteByQuery<Car>(index, Types.AllTypes,s  => s.FilterOn(x => x.Sold));
            Check.That(deleteResult.IsValid).IsTrue();
            Client.Refresh(index);
            var result = Client.Search<Car>(sc=>sc.Index(index).MatchAll());
            Check.That(result.Hits).HasSize(5);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void DeleteByQuery_FilterCreatedSeparately()
        {
            var index = AddSimpleTestData();
            var filter = Filters.CreateFilter<Car>(x => x.EngineType == EngineType.Diesel);
            var deleteResult = Client.DeleteByQuery<Car>(index, Types.AllTypes, s => s.FilterOn(filter));
            Check.That(deleteResult.IsValid).IsTrue();
            Client.Refresh(index);
            var result = Client.Search<Car>(sc => sc.Index(index).MatchAll());
            Check.That(result.Hits).HasSize(5);
            Client.DeleteIndex(index);
        }
    }
}
