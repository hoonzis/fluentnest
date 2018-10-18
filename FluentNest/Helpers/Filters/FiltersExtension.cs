using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Nest;

namespace FluentNest
{
    public static class FiltersExtension
    {
        public static QueryContainer GenerateFilterDescription<T>(this Expression expression) where T : class
        {
            var expType = expression.NodeType;

            if (expType == ExpressionType.AndAlso)
            {
                var binaryExpression = (BinaryExpression)expression;

                // handle special cases of two comparisons on the same field which should be compiled into a range request
                if (binaryExpression.Left.NodeType.IsComparisonType() && binaryExpression.Right.NodeType.IsComparisonType())
                {
                    if (binaryExpression.Left.GetFieldName() == binaryExpression.Right.GetFieldName())
                    {
                        //we supose that on left hand and right hand we have a binary expressions
                        var leftBinary = (BinaryExpression)binaryExpression.Left;
                        var leftValue = leftBinary.GetValue();

                        var fieldName = leftBinary.Left.GetFieldNameFromMemberOrGetFieldNamed();

                        var rightBinary = (BinaryExpression)binaryExpression.Right;
                        var rightValue = rightBinary.GetValue();
                        return Ranges.GenerateRangeFilter<T>(fieldName, leftValue, leftBinary.NodeType, rightValue, rightBinary.NodeType);
                    }
                }

                var rightFilter = GenerateFilterDescription<T>(binaryExpression.Right);
                var filterDescriptor = new QueryContainerDescriptor<T>();


                // Detecting a series of And filters
                var leftSide = binaryExpression.Left;
                var accumulatedExpressions = new List<Expression>();
                while (leftSide.NodeType == ExpressionType.AndAlso)
                {

                    var asBinary = (BinaryExpression)leftSide;
                    if (asBinary.Left.NodeType != ExpressionType.AndAlso)
                    {
                        accumulatedExpressions.Add(asBinary.Left);
                    }

                    if (asBinary.Right.NodeType != ExpressionType.AndAlso)
                    {
                        accumulatedExpressions.Add(asBinary.Right);
                    }

                    leftSide = asBinary.Left;
                }

                if (accumulatedExpressions.Count > 0)
                {
                    var filters = accumulatedExpressions.Select(GenerateFilterDescription<T>).ToList();
                    filters.Add(rightFilter);
                    return filterDescriptor.Bool(s => s.Must(filters.ToArray()));
                }

                var leftFilter = GenerateFilterDescription<T>(binaryExpression.Left);
                return filterDescriptor.Bool(s => s.Must(leftFilter, rightFilter));
            }

            if (expType == ExpressionType.Or || expType == ExpressionType.OrElse)
            {
                var binaryExpression = (BinaryExpression)expression;
                var leftFilter = GenerateFilterDescription<T>(binaryExpression.Left);
                var rightFilter = GenerateFilterDescription<T>(binaryExpression.Right);
                var filterDescriptor = new QueryContainerDescriptor<T>();
                return filterDescriptor.Bool(x => x.Should(leftFilter, rightFilter).MinimumShouldMatch(1));
            }

            if (expType == ExpressionType.Equal)
            {
                return (expression as BinaryExpression).GenerateEqualityFilter<T>();
            }

            if (expType == ExpressionType.LessThan || expType == ExpressionType.GreaterThan || expType == ExpressionType.LessThanOrEqual || expType == ExpressionType.GreaterThanOrEqual)
            {
                return expression.GenerateComparisonFilter<T>(expType);
            }

            if (expType == ExpressionType.MemberAccess)
            {
                var memberExpression = (MemberExpression)expression;
                //here we handle binary expressions in the from .field.hasValue
                if (memberExpression.Member.Name == "HasValue")
                {
                    var parentFieldExpression = (memberExpression.Expression as MemberExpression);
                    var parentFieldName = parentFieldExpression.GetFieldNameFromMemberOrGetFieldNamed();

                    var filterDescriptor = new QueryContainerDescriptor<T>();
                    return filterDescriptor.Exists(x => x.Field(parentFieldName));
                }

                var isProperty = memberExpression.Member.MemberType == MemberTypes.Property;

                if (isProperty)
                {
                    var propertyType = ((PropertyInfo)memberExpression.Member).PropertyType;
                    if (propertyType == typeof(bool))
                    {
                        return memberExpression.GenerateBoolFilter<T>();
                    }
                }
            }

            if (expType == ExpressionType.Call)
            {
                var callExpression = (MethodCallExpression)expression;
                if (callExpression.Method.ReturnType == typeof(bool) && expression.GetNameFromGetFieldNamed() != null)
                {
                    return callExpression.GenerateBoolFilter<T>();
                }
            }

            if (expType == ExpressionType.Lambda)
            {
                var lambda = (LambdaExpression)expression;
                return GenerateFilterDescription<T>(lambda.Body);
            }

            if (expType == ExpressionType.NotEqual)
            {
                return (expression as BinaryExpression).GenerateNotEqualFilter<T>();
            }

            throw new NotImplementedException();
        }
    }
}