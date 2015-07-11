using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentNest;
using Nest;
using NFluent;
using TestModel;
using Xunit;

namespace Tests
{
    public class HistogramTests
    {
        private ElasticClient client;

        public HistogramTests()
        {
            var node = new Uri("http://localhost:9200");

            var settings = new ConnectionSettings(
                node,
                defaultIndex: "my-application"
            );

            client = new ElasticClient(settings);
        }

        private void AddSimpleTestData()
        {
            client.DeleteIndex(x => x.Index<Car>());
            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Timestamp = new DateTime(2010,i+1,1),
                    Name = "Car" + i,
                    Price = 10,
                    Sold = i % 2 == 0 ? true : false,
                    CarType = "Type" + i%3
                };
                client.Index(car);
            }
            client.Flush(x => x.Index<Car>());
        }

        [Fact]
        public void MonthlyHistogramPerCarType()
        {
            AddSimpleTestData();
            var histogramsPerCarType =
                client.Search<Car>(
                    s =>
                        s.Aggregations(
                            a => a.DateHistogram(x => x.Timestamp, DateInterval.Month).GroupBy(x => x.CarType)));

            var carTypes =
                histogramsPerCarType.Aggs.GetDictioanry<Car, IList<HistogramItem>>(x => x.CarType,
                    v => v.GetDateHistogram<Car>(f => f.Timestamp));

            Check.That(carTypes).HasSize(3);

            var firstType = carTypes["type0"];
            Check.That(firstType).HasSize(4);
        }

        [Fact]
        public void SumInMonthlyHistogram()
        {
            AddSimpleTestData();
            var sumOnPrice = Sums.SumOnField<Car>(x => x.Price);
            var esResult =
                client.Search<Car>(
                    search => search.Aggregations(x => sumOnPrice.IntoDateHistogram(date => date.Timestamp, DateInterval.Month)));

            var histogram = esResult.Aggs.GetDateHistogram<Car>(x => x.Timestamp);
            Check.That(histogram).HasSize(10);
            Check.That(histogram.All(x => x.GetSum<Car>(s => s.Price).Value == 10d)).IsTrue();
        }

        [Fact]
        public void MonthlySumHistogramFilteredOnDates()
        {
            AddSimpleTestData();
            var start = new DateTime(2010, 1, 1);
            var end = new DateTime(2010, 4, 4);
            var sumOnSomeNotional = Sums.SumOnField<Car>(x => x.Price);
            var esResult =
                client.Search<Car>(
                    search => search.FilteredOn(f => f.Timestamp < end && f.Timestamp > start).
                        Aggregations(x => sumOnSomeNotional.IntoDateHistogram(date => date.Timestamp, DateInterval.Month)));

            var histogram = esResult.Aggs.GetDateHistogram<Car>(x => x.Timestamp);

            Check.That(histogram).HasSize(3);
        }
    }
}
