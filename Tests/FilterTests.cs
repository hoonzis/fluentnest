using System;
using FluentNest;
using Nest;
using NFluent;
using TestModel;
using Xunit;

namespace Tests
{
    public class FilterTests
    {
        private ElasticClient client;

        class User
        {
            [ElasticProperty(OmitNorms = true, Index = FieldIndexOption.NotAnalyzed)]
            public String Email { get; set; }

            public String Name { get; set; }

            public int Age { get; set; }

            public bool? Enabled { get; set; }

            public bool Active { get; set; }

        }

        
        public FilterTests()
        {
            var node = new Uri("http://localhost:9600");

            var settings = new ConnectionSettings(
                node,
                defaultIndex: "my-application"
            );
            client = new ElasticClient(settings);
        }

        private void AddSimpleTestData()
        {
            client.DeleteIndex(x => x.Index<Car>());
            client.DeleteIndex(x => x.Index("test"));
            var createIndexResult = client.CreateIndex("test", x => x.AddMapping<User>(c => c.MapFromAttributes()));

            Check.That(createIndexResult.Acknowledged).IsTrue();
            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Timestamp = new DateTime(2010,(i%12)+1,1),
                    Name = "Car" + i,
                    Price = 10,
                    Sold = i % 2 == 0 ? true : false,
                    CarType = "Type" + i%2,
                };
                client.Index(car);
            }

            for (int i = 0; i < 10; i++)
            {
                var user = new User
                {
                    Email = "Email@email"+i%2+".com",
                    Name = "name"+i%3,
                    Age = i+1,
                    Enabled = i%2 == 0 ? true : false,
                    Active = i % 2 == 0 ? true : false
                };
                client.Index(user, c => c.Index("test"));
            }
            client.Flush(x => x.Index("test"));
            client.Flush(x => x.Index<Car>());
        }


        [Fact]
        public void DateComparisonAndTerm()
        {
            AddSimpleTestData();

            var startDate = new DateTime(2010, 1, 1);
            var endDate = new DateTime(2010, 3, 1);
           
            var result = client.Search<Car>(s => s.FilterOn(x => x.Timestamp >= startDate && x.Timestamp <= endDate && x.CarType == "type0"));


            Check.That(result.Documents).HasSize(2);
        }

        [Fact]
        public void DateEqualityTest()
        {
            AddSimpleTestData();

            var startDate = new DateTime(2010, 1, 1);

            var result = client.Search<Car>(s => s.FilterOn(x => x.Timestamp == startDate));
            
            Check.That(result.Documents).HasSize(1);
        }

        [Fact]
        public void TestSimpleComparisonFilter()
        {
            AddSimpleTestData();
            var startDate = new DateTime(2010, 1, 1);
            var endDate = new DateTime(2010, 3, 1);
            var result = client.Search<Car>(s => s
                .FilterOn(x => x.Timestamp > startDate && x.Timestamp < endDate));
            Check.That(result.Documents).HasSize(1);
        }

        [Fact]
        public void TestEqualityFilter()
        {
            AddSimpleTestData();

            var carType = "Type0".ToLower();
            //Standard Nest way of getting the docuements. Values are lowered by ES
            var result = client.Search<Car>(s => s.Filter(x => x.Term(f => f.CarType, carType)));
            Check.That(result.Documents).HasSize(5);
            
            //Best way
            result = client.Search<Car>(s => s.FilterOn(x => x.CarType == carType));
            Check.That(result.Documents).HasSize(5);

        }

        [Fact]
        public void TestRangeFilters_And_And()
        {
            AddSimpleTestData();

            var startDate = new DateTime(2010, 1, 1);
            var endDate = new DateTime(2010, 5, 1);
         
            var result = client.Search<Car>(s => s.Query(
                    q=>q.Filtered(fil=>fil.Filter(
                        x => x.And(
                            left=>left.Range(f=>f.OnField(fd=>fd.Timestamp).Greater(startDate)),
                            right=>right.Range(f=>f.OnField(fd=>fd.Timestamp).Lower(endDate))
                        )
                    )
                )
            ));
            Check.That(result.Documents).HasSize(3);

            //Much better
            result = client.Search<Car>(s => s.FilteredOn(f => f.Timestamp > startDate && f.Timestamp < endDate));
            Check.That(result.Documents).HasSize(3);
        }

        [Fact]
        public void FilterOnSpecialCharacter()
        {
            AddSimpleTestData();

            var allUsers = client.Search<User>(s => s.Index("test"));
            Check.That(allUsers.Documents).HasSize(10);

            //these two searches should provide the same result
            var result =
                client.Search<User>(
                    s => s.Index("test").Query(q => q.Filtered(f => f.Filter(fil => fil.Term(term => term.Email, "Email@email1.com")))));
            Check.That(result.Documents).HasSize(5);

            result = client.Search<User>(s => s.Index("test").FilteredOn(f => f.Email == "Email@email1.com"));
            Check.That(result.Documents).HasSize(5);
        }

        [Fact]
        public void TestConsecutiveFilters()
        {
            AddSimpleTestData();

            var filter = NestHelperMethods
                .CreateFilter<User>(x => x.Name == "name1" && x.Age >= 5)
                .AndFilteredOn<User>(x => x.Email == "Email@email1.com");

            var allUsers = client.Search<User>(s => s.Index("test").Filter(filter));
            Check.That(allUsers.Documents).HasSize(1);            
        }

        [Fact]
        public void TestBooleanFilter()
        {
            AddSimpleTestData();

            var filter = NestHelperMethods
                .CreateFilter<User>(x => x.Enabled == true);

            var allUsers = client.Search<User>(s => s.Index("test").Filter(filter));
            Check.That(allUsers.Documents).HasSize(5);
        }

        [Fact]
        public void MultipleFiltersAndSomeAggregations()
        {
            AddSimpleTestData();

            var sc = new SearchDescriptor<User>();
            sc = sc.Index("test");

            var filter = NestHelperMethods
                .CreateFilter<User>(x => x.Name == "name1" && x.Age >= 5)
                .AndFilteredOn<User>(x => x.Email == "Email@email1.com");

            var ageSum  = new AggregationDescriptor<User>().SumBy(x => x.Age);

            sc = sc.FilteredOn(filter).Aggregations(agg => ageSum);

            var filterdAggregation = client.Search<User>(sc);
            var sumValue = filterdAggregation.Aggs.GetSum<User, int>(x => x.Age);

            var aggsContainer = filterdAggregation.Aggs.AsContainer<User>();
            var sum2 = aggsContainer.GetSum(x => x.Age);
            Check.That(sumValue).Equals(8);
            Check.That(sum2).Equals(8);
        }

        [Fact]
        public void Or_Filter_Test()
        {
            AddSimpleTestData();

            var filter = NestHelperMethods
                .CreateFilter<User>(x => x.Name == "name1" || x.Age >= 5);

            var allUsers = client.Search<User>(s => s.Index("test").Filter(filter));
            Check.That(allUsers.Documents).HasSize(7);
        }

        [Fact]
        public void Noq_equal_Filter_Test()
        {
            AddSimpleTestData();

            var filter = NestHelperMethods
                .CreateFilter<User>(x => x.Name != "name1" && x.Name != "name2");

            var allUsers = client.Search<User>(s => s.Index("test").Filter(filter));
            Check.That(allUsers.Documents).HasSize(4);
        }

        [Fact]
        public void Bool_filter_test()
        {
            AddSimpleTestData();

            var filter = NestHelperMethods
                .CreateFilter<User>(x => x.Active);

            var allUsers = client.Search<User>(s => s.Index("test").Filter(filter));
            Check.That(allUsers.Documents).HasSize(5);
        }

        [Fact]
        public void Time_equality_filter()
        {
            AddSimpleTestData();
            
            var allUsers = client.Search<Car>(s=>s.FilterOn(x=>x.Timestamp == new DateTime(2010,1,1)));
            Check.That(allUsers.Documents).HasSize(1);
        }
    }
}

