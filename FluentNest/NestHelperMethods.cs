using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq.Expressions;
using System.Reflection;
using Nest;

namespace FluentNest
{
    public static class NestHelperMethods
    {
        public static string FirstCharacterToLower(this string str)
        {
            if (String.IsNullOrEmpty(str) || Char.IsLower(str, 0))
                return str;

            return Char.ToLowerInvariant(str[0]) + str.Substring(1);
        }

        public static AggregationContainerDescriptor<T> IntoDateHistogram<T>(this AggregationContainerDescriptor<T> innerAggregation,
            Expression<Func<T, Object>> fieldGetter, DateInterval interval) where T : class
        {
            AggregationContainerDescriptor<T> v = new AggregationContainerDescriptor<T>();
            var fieldName = GetName(fieldGetter);
            v.DateHistogram(fieldName, dr =>
            {
                DateHistogramAggregationDescriptor<T> dateAggDesc = new DateHistogramAggregationDescriptor<T>();
                dateAggDesc.Field(fieldGetter).Interval(interval);
                return dateAggDesc.Aggregations(x => innerAggregation);
            });

            return v;
        }

        public static AggregationContainerDescriptor<T> IntoHistogram<T>(this AggregationContainerDescriptor<T> innerAggregation,
            Expression<Func<T, Object>> fieldGetter, int interval) where T : class
        {
            AggregationContainerDescriptor<T> v = new AggregationContainerDescriptor<T>();
            var fieldName = GetName(fieldGetter);
            v.Histogram(fieldName, dr =>
            {
                HistogramAggregationDescriptor<T> dateAggDesc = new HistogramAggregationDescriptor<T>();
                dateAggDesc.Field(fieldGetter).Interval(interval);
                return dateAggDesc.Aggregations(x => innerAggregation);
            });

            return v;
        }

        public static AggregationContainerDescriptor<T> DateHistogram<T>(this AggregationContainerDescriptor<T> agg,
            Expression<Func<T, Object>> fieldGetter, DateInterval dateInterval) where T : class
        {
            return agg.DateHistogram(GetName(fieldGetter), x => x.Field(fieldGetter).Interval(dateInterval));
        }

        private static string GetName<T, K>(this Expression<Func<T, K>> exp)
        {
            MemberExpression body = exp.Body as MemberExpression;

            if (body == null)
            {
                UnaryExpression ubody = (UnaryExpression) exp.Body;
                body = ubody.Operand as MemberExpression;
            }


            return body.Member.Name;
        }

        public static string GetAggName<T, K>(this Expression<Func<T, K>> exp, AggType type)
        {
            MemberExpression body = exp.Body as MemberExpression;

            if (body == null)
            {
                UnaryExpression ubody = (UnaryExpression) exp.Body;
                body = ubody.Operand as MemberExpression;
            }


            return type + body.Member.Name;
        }


        public static IList<DateHistogramItem> GetDateHistogram<T>(this KeyedBucket item,
            Expression<Func<T, Object>> fieldGetter)
        {
            var histogramItem = item.DateHistogram(GetName(fieldGetter));
            return histogramItem.Items;
        }

        public static IList<DateHistogramItem> GetDateHistogram<T>(this AggregationsHelper aggs,
            Expression<Func<T, Object>> fieldGetter)
        {
            var histogramItem = aggs.DateHistogram(GetName(fieldGetter));
            return histogramItem.Items;
        }

        public static IList<HistogramItem> GetHistogram<T>(this AggregationsHelper aggs,
            Expression<Func<T, Object>> fieldGetter)
        {
            var histogramItem = aggs.Histogram(GetName(fieldGetter));
            return histogramItem.Items;
        }

        public static QueryContainer GenerateComparisonFilter<T>(this Expression expression, ExpressionType type)
            where T : class
        {
            var binaryExpression = expression as BinaryExpression;

            var value = GetValue(binaryExpression.Right);
            var memberAccessor = binaryExpression.Left as MemberExpression;
            var fieldName = GetFieldNameFromMember(memberAccessor);

            if (value is DateTime)
            {
                return GenerateComparisonFilter<T>((DateTime) value, type, fieldName);
            }
            else if (value is double || value is decimal)
            {
                return GenerateComparisonFilter<T>(Convert.ToDouble(value), type, fieldName);
            }
            else if (value is int || value is long)
            {
                return GenerateComparisonFilter<T>(Convert.ToInt64(value), type, fieldName);
            }
            throw new InvalidOperationException("Comparison on non-supported type");
        }

        public static QueryContainer GenerateComparisonFilter<T>(DateTime value, ExpressionType type, string fieldName)
            where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            if (type == ExpressionType.LessThan)
            {
                return filterDescriptor.DateRange(x => x.LessThan(value).Field(fieldName));
            }
            else if (type == ExpressionType.GreaterThan)
            {
                return filterDescriptor.DateRange(x => x.GreaterThan(value).Field(fieldName));
            }
            else if (type == ExpressionType.LessThanOrEqual)
            {
                return filterDescriptor.DateRange(x => x.LessThanOrEquals(value).Field(fieldName));
            }
            else if (type == ExpressionType.GreaterThanOrEqual)
            {
                return filterDescriptor.DateRange(x => x.GreaterThanOrEquals(value).Field(fieldName));
            }
            throw new NotImplementedException();
        }

        public static QueryContainer GenerateComparisonFilter<T>(long value, ExpressionType type, string fieldName)
            where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            if (type == ExpressionType.LessThan)
            {
                return filterDescriptor.Range(x => x.LessThan(value).Field(fieldName));
            }
            else if (type == ExpressionType.GreaterThan)
            {
                return filterDescriptor.Range(x => x.GreaterThan(value).Field(fieldName));
            }
            else if (type == ExpressionType.LessThanOrEqual)
            {
                return filterDescriptor.Range(x => x.LessThanOrEquals(value).Field(fieldName));
            }
            else if (type == ExpressionType.GreaterThanOrEqual)
            {
                return filterDescriptor.Range(x => x.GreaterThanOrEquals(value).Field(fieldName));
            }
            throw new NotImplementedException();
        }

        public static QueryContainer GenerateComparisonFilter<T>(double value, ExpressionType type, string fieldName)
            where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            if (type == ExpressionType.LessThan)
            {
                return filterDescriptor.Range(x => x.LessThan(value).Field(fieldName));
            }
            else if (type == ExpressionType.GreaterThan)
            {
                return filterDescriptor.Range(x => x.GreaterThan(value).Field(fieldName));
            }
            else if (type == ExpressionType.LessThanOrEqual)
            {
                return filterDescriptor.Range(x => x.LessThanOrEquals(value).Field(fieldName));
            }
            else if (type == ExpressionType.GreaterThanOrEqual)
            {
                return filterDescriptor.Range(x => x.GreaterThanOrEquals(value).Field(fieldName));
            }
            throw new NotImplementedException();
        }

        public static QueryContainer GenerateEqualityFilter<T>(this Expression expression) where T : class
        {
            var binaryExpression = expression as BinaryExpression;
            var value = GetValue(binaryExpression.Right);
            var queryContainerDescriptor = new QueryContainerDescriptor<T>();
            var fieldName = GetFieldName(binaryExpression.Left);
            return queryContainerDescriptor.Term(fieldName, value);
        }

        public static QueryContainer GenerateNotEqualFilter<T>(this Expression expression) where T : class
        {
            var equalityFilter = GenerateEqualityFilter<T>(expression);
            var filterDescriptor = new QueryContainerDescriptor<T>();
            return filterDescriptor.Bool(x => x.MustNot(equalityFilter));
        }

        public static QueryContainer GenerateBoolFilter<T>(this Expression expression) where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            var fieldName = GenerateFilterName(expression);
            return filterDescriptor.Term(fieldName, true);
        }


        public static string GetFieldName(this Expression expression)
        {
            if (expression is MemberExpression)
            {
                return GetFieldNameFromMember(expression as MemberExpression);
            }
            else if (expression is UnaryExpression)
            {
                var unary = expression as UnaryExpression;
                return GetFieldName(unary.Operand);
            }
            throw new NotImplementedException();
        }

        public static string GetFieldNameFromMember(this MemberExpression expression)
        {
            return FirstCharacterToLower(expression.Member.Name);
        }

        public static QueryContainer GenerateFilterDescription<T>(this Expression expression) where T:class
        {
            var expType = expression.NodeType;
            
            if (expType == ExpressionType.AndAlso)
            {
                var binaryExpression = expression as BinaryExpression;
                var leftFilter = GenerateFilterDescription<T>(binaryExpression.Left);
                var rightFilter = GenerateFilterDescription<T>(binaryExpression.Right);
                var filterDescriptor = new QueryContainerDescriptor<T>();
                return filterDescriptor.Bool(s => s.Must(leftFilter).Must(rightFilter));

            }
            else if (expType == ExpressionType.Or || expType == ExpressionType.OrElse)
            {
                var binaryExpression = expression as BinaryExpression;
                var leftFilter = GenerateFilterDescription<T>(binaryExpression.Left);
                var rightFilter = GenerateFilterDescription<T>(binaryExpression.Right);
                var filterDescriptor = new QueryContainerDescriptor<T>();
                return filterDescriptor.Bool(x => x.Should(leftFilter).Should(rightFilter).MinimumShouldMatch(1));
            }
            else if (expType == ExpressionType.Equal)
            {
                return GenerateEqualityFilter<T>(expression);
            }
            else if(expType == ExpressionType.LessThan || expType == ExpressionType.GreaterThan || expType == ExpressionType.LessThanOrEqual || expType == ExpressionType.GreaterThanOrEqual)
            {
                return GenerateComparisonFilter<T>(expression,expType);
            }
            else if (expType == ExpressionType.MemberAccess)
            {
                var memberExpression = expression as MemberExpression;
                //here we handle binary expressions in the from .field.hasValue
                if (memberExpression.Member.Name == "HasValue")
                {
                    var parentFieldExpression = (memberExpression.Expression as MemberExpression);
                    var parentFieldName = GetFieldNameFromMember(parentFieldExpression);

                    var filterDescriptor = new QueryContainerDescriptor<T>();
                    return filterDescriptor.Exists(x => x.Field(parentFieldName));
                }
                var isProperty = memberExpression.Member.MemberType == MemberTypes.Property;
                if (isProperty)
                {
                    var propertyType = ((PropertyInfo) memberExpression.Member).PropertyType;
                    if (propertyType == typeof (bool))
                    {
                        return GenerateBoolFilter<T>(memberExpression);
                    }
                }
            }
            else if (expType == ExpressionType.Lambda)
            {
                var lambda = expression as LambdaExpression;
                return GenerateFilterDescription<T>(lambda.Body);
            }else if (expType == ExpressionType.NotEqual)
            {
                return GenerateNotEqualFilter<T>(expression);
            }
            throw  new NotImplementedException();
        }

        public static string GenerateFilterName(this Expression expression)
        {
            var expType = expression.NodeType;

            if (expType == ExpressionType.AndAlso || expType == ExpressionType.Or || expType == ExpressionType.OrElse || expType == ExpressionType.Equal 
                || expType == ExpressionType.LessThan || expType == ExpressionType.GreaterThan || expType == ExpressionType.LessThanOrEqual || expType == ExpressionType.GreaterThanOrEqual
                || expType == ExpressionType.NotEqual)
            {
                var binaryExpression = expression as BinaryExpression;
                var leftFilterName = GenerateFilterName(binaryExpression.Left);
                var rightFilterName = GenerateFilterName(binaryExpression.Right);
                return leftFilterName + "_" + expType + "_" + rightFilterName;
            }
            else if (expType == ExpressionType.MemberAccess)
            {
                var memberExpression = expression as MemberExpression;
                //here we handle binary expressions in the from .field.hasValue
                if (memberExpression.Member.Name == "HasValue")
                {
                    var parentFieldExpression = (memberExpression.Expression as MemberExpression);
                    var parentFieldName = GetFieldNameFromMember(parentFieldExpression);
                    return parentFieldName + ".hasValue";
                }
                return GetFieldNameFromMember(memberExpression);
            }
            else if (expType == ExpressionType.Lambda)
            {
                var lambda = expression as LambdaExpression;
                return GenerateFilterName(lambda.Body);
            }
            else if (expType == ExpressionType.Convert)
            {
                var unary = expression as UnaryExpression;
                return GenerateFilterName(unary.Operand);
            }else if (expType == ExpressionType.Constant)
            {
                var constExp = expression as ConstantExpression;
                return constExp.Value.ToString();
            }
            throw new NotImplementedException();
        }

        public static SearchDescriptor<T> FilterOn<T>(this SearchDescriptor<T> searchDescriptor, Expression<Func<T, bool>> filterRule) where T : class
        {
            var binaryExpression = filterRule.Body as BinaryExpression;
            var filterDescriptor = GenerateFilterDescription<T>(binaryExpression);
            return searchDescriptor.Query(_ => filterDescriptor);
        }

        public static SearchDescriptor<T> FilteredOn<T>(this SearchDescriptor<T> searchDescriptor, Expression<Func<T, bool>> filterRule) where T : class
        {           
            var binaryExpression = filterRule.Body as BinaryExpression;            
            return searchDescriptor.Query(q => q.Filtered(fil=>fil.Filter(f=>GenerateFilterDescription<T>(binaryExpression))));
        }

        public static SearchDescriptor<T> FilteredOn<T>(this SearchDescriptor<T> searchDescriptor, QueryContainer container) where T : class
        {
            return searchDescriptor.Query(q => q.Bool(b => b.Must(container)));
        }

        public static DeleteByQueryDescriptor<T> FilteredOn<T>(this DeleteByQueryDescriptor<T> deleteDescriptor, QueryContainer container) where T : class
        {
            return deleteDescriptor.Query(q => q.Bool(b => b.Must(container)));
        }

        public static DeleteByQueryDescriptor<T> FilteredOn<T>(this DeleteByQueryDescriptor<T> deleteDescriptor, Expression<Func<T, bool>> filterRule) where T : class
        {
            var binaryExpression = filterRule.Body as BinaryExpression;
            var deleteByQueryDescriptor = deleteDescriptor.Query(q => q.Bool(b => b.Must(GenerateFilterDescription<T>(binaryExpression))));
            return deleteByQueryDescriptor;
        }


        public static QueryContainer AndFilteredOn<T>(this QueryContainer queryDescriptor, Expression<Func<T, bool>> filterRule) where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            var binaryExpression = filterRule.Body as BinaryExpression;
            var newPartOfQuery = GenerateFilterDescription<T>(binaryExpression);
            return filterDescriptor.Bool(x => x.Must(queryDescriptor).Must(newPartOfQuery));            
        }

        public static QueryContainer CreateFilter<T>(Expression<Func<T, bool>> filterRule) where T : class
        {
            return GenerateFilterDescription<T>(filterRule);
        }

        private static object GetValue(Expression member)
        {
            var convertedMember = ExplicitlyConvertEnums(member);

            var objectMember = Expression.Convert(convertedMember, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            var value = getter();
            return value;
        }

        /// <summary>
        /// This is necessary in order to avoid the automatic cast of enums to the underlying integer representation
        /// </summary>
        private static Expression ExplicitlyConvertEnums(Expression member)
        {
            var unaryExpression = member as UnaryExpression;
            if (unaryExpression != null && unaryExpression.Operand.Type.IsEnum)
            {
                return Expression.Convert(member, unaryExpression.Operand.Type);
            }

            return member;
        }

        public static T Parse<T>(string value)
        {
            return (T)Enum.Parse(typeof(T), value);
        }

        public static AggsContainer<T> AsContainer<T> (this AggregationsHelper aggs)
        {
            return new AggsContainer<T>(aggs);
        }

        public static K StringToAnything<K>(string value)
        {
            if ((typeof(K).IsEnum))
            {
                return NestHelperMethods.Parse<K>(value);
            }
            else
            {
                TypeConverter typeConverter = TypeDescriptor.GetConverter(typeof(K));
                return (K)typeConverter.ConvertFromString(value);
            }
        }
    }
}