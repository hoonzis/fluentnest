# FluentNest
LINQ-like query language for ElasticSearch built on top of NEST.

NEST for querying ElasticSearch is great, but sometimes still hard to read and not very LINQ-like. This library contains set of methods that give you more LINQ-like feeling. Currently mainly aggregations and filters are covered.

Aggregations nested in Groups
-----------------------------
Quite often you might want to calculate a sum per group. Standard way of doing this with NEST would be:
```Csharp
var result = client.Search<Car>(s => s
	.Aggregations(fstAgg => fstAgg
		.Terms("firstLevel", f => f
			.Field(z => z.CarType)
				.Aggregations(sums => sums
					.Sum("priceSum", son => son
					.Field(f4 => f4.Price)
				)
			)
		)
	)
);
```

With **FluentNest** you can write:

```Csharp
groupedSum = Statistics
	.SumBy<Car>(s => s.Price)
	.GroupBy(s => s.EngineType);
	
var result = client.Search<Car>(search => search.Aggregations(x => groupedSum);
```

Nested groups are very easy as well:

```Csharp
groupedSum = Statistics
	.SumBy<Car>(s => s.Price)
	.GroupBy(s => s.CarType)
	.GroupBy(s => s.EngineType)
```

Other helper methods will allow you to unwrap what you need from the ElastiSearch query result.

```Csharp
var carTypes = result.Aggs.GetGroupBy<Car>(x => x.CarType);
foreach (var carType in carTypes)
{
	var engineTypes = carType.GetGroupBy<Car>(x => x.EngineType);
	//do somethign with nested types
}
```

Dynamic nested grouping
-----------------------
In some cases you might need to group dynamically on multiple criteria specified at run-time. For such cases there is an overload of GroupBy which takes the name of the field to used for grouping. This overload can be used to obtain nested grouping on a list of fields:

```Csharp
var agg = Statistics
	.SumBy<Car>(s => s.Price)
	.GroupBy(new List<string> {"engineType", "carType"});

var result = client.Search<Car>(search => search.Aggregations(x => agg));
```

Conditional statistics
----------------------
Typically conditional sums, can be quite complicated with NEST, since you will have to define multiple *Filter* aggregations with each of them having inner *Sum* aggregation. Here is a quicker way:

```Csharp
var aggs = Statistics
	.CondSumBy<Car>(x=>x.Price, c=>c.EngineType == "Engine1")
	.AndCondSumBy<Car>(x=>x.Sales, c=>c.CarType == "Car1");
	
var result = client.Search<Car>(search => search.Aggregations(a=>aggs)):
```

Expressions to queries
----------------------
Filtering on multiple conditions is complicated since you will have to use *Or*, *And*, *Range* and other methods. **FluentNest** can compile some small and easy expressions into NEST query language:

```Csharp
result = client.Search<Car>(s => s.FilteredOn(f => f.Timestamp > startDate && f.Timestamp < endDate));
```

Multiple statistics
-------------------
You can defined and obtain multiple statistics in the same time. This has been already shown in the previous snippet of code. The "And" notation can be used to obtain multiple statistics on the same level:

```Csharp
var aggs = Statistics
	.SumBy(x=>x.Price)
	.AndCardinalityBy(x=>x.EngineType)
	.AndCondCountBy(x=>x.Name, c=>c.EngineType == "Engine1");
```

Hitograms
---------
Histogram is another useful aggregation supported by ElasticSearch. Here is a way to get a *Sum* by month.

```Csharp
var agg = Statistics
	.SumBy<Car>(x => x.Price)
	.IntoDateHistogram(date => date.Timestamp, DateInterval.Month);

var result = client.Search<Car>(s => s.Aggregations(a =>agg);
var histogram = result.Aggs.GetDateHistogram<Car>(x => x.Timestamp);
```
