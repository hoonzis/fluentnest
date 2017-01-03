# FluentNest

Fluent way to query ElasticSearch from C# based on  [NEST](https://github.com/elastic/elasticsearch-net).

| Elastic Search & Nest Version | Build | Nuget | Branch |
| ----------------------------- |:-----:| :----:|:------:|
| 5.x | [![Build 5.x](https://ci.appveyor.com/api/projects/status/wrorpoekyw416hn1?svg=true)](https://ci.appveyor.com/project/hoonzis/fluentnest)|[![Nuget Package](https://img.shields.io/nuget/v/fluentnest.svg)](https://www.nuget.org/packages/fluentnest) | [master](https://github.com/hoonzis/fluentnest/) |
| 2.x and greater| [![Build 2.x](https://ci.appveyor.com/api/projects/status/wrorpoekyw416hn1/branch/2.x?svg=true)](https://ci.appveyor.com/project/hoonzis/fluentnest)|[![Nuget Package](https://img.shields.io/badge/nuget-v2.0.217-blue.svg)](https://www.nuget.org/packages/FluentNest/1.0.217) | [2.x](https://github.com/hoonzis/fluentnest/) |
| 1.7.x and greater| [![Build 1.7.x](https://ci.appveyor.com/api/projects/status/wrorpoekyw416hn1/branch/1.0.0?svg=true)](https://ci.appveyor.com/project/hoonzis/fluentnest) | [![Nuget Package](https://img.shields.io/badge/nuget-v1.0.210-blue.svg)](https://www.nuget.org/packages/FluentNest/1.0.210) | [1.x](https://github.com/hoonzis/fluentnest/tree/1.0.0) |

Complex queries in ElasticSearch JSON query language are barely readable. NEST takes some part of the pain away, but nested lambdas are still painful. *FluenNest* tries to simplify the query composition. Details are available in the [wiki](https://github.com/hoonzis/fluentnest/wiki/FluentNest-wiki). Motivation and few implementation details are described in [this blog post.](http://www.hoonzis.com/fluent-interface-for-elastic-search/)

### Statistics
```csharp
var result = client.Search<Car>(sc => sc.Aggregations(agg => agg
	.SumBy(x => x.Price)
	.CardinalityBy(x => x.EngineType)
);

var container = result.Aggs.AsContainer<Car>();
var priceSum = container.GetSum(x => x.Price);
var engines = container.GetCardinality(x => x.EngineType);
```

### Conditional statistics
Conditional sums can be quite complicated with NEST. One has to define a **Filter** aggregation with nested inner **Sum** or other aggregation. Here is quicker way with FluentNest:

```CSharp
var result = client.Search<Car>(sc => sc.Aggregations(aggs => aggs
	.SumBy(x=>x.Price, c => c.EngineType == EngineType.Diesel)
	.SumBy(x=>x.Sales, c => c.CarType == "Car1"))
);
```

### Filtering and expressions to queries compilation
Filtering on multiple conditions might be complicated since you have to compose filters using **Bool** together with **Must** or **Should** methods, often resulting in huge lambdas. *FluentNest* can compile small expressions into NEST query language:

```csharp
client.Search<Car>(s => s.FilterOn(f => f.Timestamp > startDate && f.Timestamp < endDate));
client.Search<Car>(s => s.FilterOn(f=> f.Ranking.HasValue || f.IsAllowed);
client.Search<Car>(s => s.FilterOn(f=> f.Age > 10 || f.Age < 20 && f.Name == "Peter");
```

### Groups & Histograms
Quite often you might want to calculate an aggregation per group or per histogram bucket:

```csharp
var result = client.Search<Car>(sc => sc.Aggregations(agg => agg
	.SumBy(s => s.Price)
	.GroupBy(s => s.EngineType)
);
```

```csharp
var result = client.Search<Car>(s => s.Aggregations(agg => agg
	.SumBy(x => x.Price, x => x.EngineType == EngineType.Diesel)
	.IntoDateHistogram(date => date.Timestamp, DateInterval.Month)
);
```

Groups and histograms can be also nested:

```csharp
var result = client.Search<Car>(sc => sc.Aggregations(agg => agg
	.SumBy(s => s.Price)
	.GroupBy(s => s.CarType)
	.IntoDateHistogram(date => date.Timestamp, DateInterval.Month));
```
