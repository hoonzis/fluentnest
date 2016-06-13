using System;
using System.IO;
using System.Text;
using System.Threading;
using FluentNest;
using Nest;
using NFluent;
using TestModel;
using Xunit;
namespace Tests
{
    public class DeleteTests : TestsBase
    {
        public void AddSimpleTestData()
        {
            client.DeleteIndex(CarIndex);
            client.CreateIndex(CarIndex, x => x.Mappings(
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

                client.Index(car);
            }
            client.Flush(CarIndex);
        }

        [Fact]
        public void DeleteByQuery()
        {
            AddSimpleTestData();
            client.DeleteByQuery<Car>(CarIndex, s => s.FilteredOn(x => x.Sold == true));
            client.Refresh(CarIndex);
            var result = client.Search<Car>(sc=>sc.Index(CarIndex).MatchAll());
            Check.That(result.Hits).HasSize(5);
        }

        [Fact]
        public void DeleteByQuery_FilterCreatedSeparately()
        {
            AddSimpleTestData();
            var filter = Filters.CreateFilter<Car>(x => x.EngineType == EngineType.Diesel);
            client.DeleteByQuery<Car>(CarIndex, s => s.FilteredOn(filter));
            client.Refresh(CarIndex);
            var result = client.Search<Car>(sc => sc.MatchAll());
            Check.That(result.Hits).HasSize(5);
        }
    }
}
