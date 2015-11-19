using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentNest;
using Nest;
using Newtonsoft.Json.Converters;
using NFluent;
using TestModel;
using Xunit;

namespace Tests
{
    public class EnumTests : TestsBase
    {
        private static readonly Func<ConnectionSettings, ConnectionSettings> AddEnumStringConverter =
            setting => setting.AddContractJsonConverters(t => t.IsEnum ? new StringEnumConverter() : null);

        public EnumTests()
            : base(AddEnumStringConverter)
        {

        }

        [Fact]
        public void Filtering_on_enum_property_should_work()
        {
            AddSimpleTestData();
            var filter = NestHelperMethods.CreateFilter<Car>(x => x.EngineType == EngineType.Diesel);

            var result = client.Search<Car>(s => s
                .Take(100).Filter(filter));

            Check.That(result.Hits.Count()).IsEqualTo(5);
        }
    }
}
