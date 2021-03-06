﻿using System;
using System.Linq.Expressions;
using System.Reflection;

namespace FluentNest
{
    public static class Names
    {
        public static TField GetFieldNamed<TField>(this object target, string name)
        {
            throw new InvalidOperationException("This method should be used in extensions.");
        }

        private static MethodInfo getFieldNamedMethod = typeof(Names).GetMethod("GetFieldNamed");

        public static string GetNameFromGetFieldNamed(Expression expression)
        {
            if (expression is UnaryExpression unaryExpression && unaryExpression.NodeType == ExpressionType.Convert)
            {
                return GetNameFromGetFieldNamed(unaryExpression.Operand);
            }

            if (!(expression is MethodCallExpression methodCall))
            {
                return null;
            }

            if (methodCall.Method.GetGenericMethodDefinition() != getFieldNamedMethod)
            {
                return null;
            }

            if (!(methodCall.Arguments[1] is Expression nameParam) || nameParam.Type != typeof(string))
            {
                return null;
            }

            return GetStringOf(nameParam);
        }

        private static string GetStringOf(Expression expr)
        {
            if (expr.Type != typeof(string))
            {
                throw new InvalidOperationException("string only");
            }

            if (expr is ConstantExpression constant)
            {
                return (string)constant.Value;
            }

            if (expr is MemberExpression memberExpr && memberExpr.Expression is ConstantExpression target)
            {
                if (memberExpr.Member.MemberType == MemberTypes.Field)
                {
                    FieldInfo fi = (FieldInfo)memberExpr.Member;
                    return (string)fi.GetValue(target.Value);
                }
                else
                {
                    PropertyInfo pi = (PropertyInfo)memberExpr.Member;
                    return (string)pi.GetValue(target.Value);
                }
            }

            if (expr is BinaryExpression methodCall && methodCall.NodeType == ExpressionType.Add)
            {
                var left = GetStringOf(methodCall.Left);
                var right = GetStringOf(methodCall.Right);
                return left + right;
            }

            var lambda = (Expression<Func<string>>)Expression.Lambda(expr);
            return lambda.Compile().Invoke();
        }

        public static string GenerateFilterName(this Expression expression)
        {
            var fromGetFieldName = GetNameFromGetFieldNamed(expression);
            if (fromGetFieldName != null)
            {
                return fromGetFieldName;
            }

            var expType = expression.NodeType;

            if (expType == ExpressionType.AndAlso || expType == ExpressionType.Or || expType == ExpressionType.OrElse || expType == ExpressionType.Equal
                || expType == ExpressionType.LessThan || expType == ExpressionType.GreaterThan || expType == ExpressionType.LessThanOrEqual || expType == ExpressionType.GreaterThanOrEqual
                || expType == ExpressionType.NotEqual)
            {
                var binaryExpression = (BinaryExpression)expression;
                var leftFilterName = GenerateFilterName(binaryExpression.Left);
                var rightFilterName = GenerateFilterName(binaryExpression.Right);
                return leftFilterName + "_" + expType + "_" + rightFilterName;
            }

            if (expType == ExpressionType.MemberAccess)
            {
                var memberExpression = (MemberExpression)expression;
                //here we handle binary expressions in the from .field.hasValue
                if (memberExpression.Member.Name == "HasValue")
                {
                    var parentFieldExpression = (memberExpression.Expression as MemberExpression);
                    var parentFieldName = parentFieldExpression.GetFieldNameFromMemberOrGetFieldNamed();
                    return parentFieldName + ".hasValue";
                }
                return memberExpression.GetFieldNameFromMemberOrGetFieldNamed();
            }
            if (expType == ExpressionType.Lambda)
            {
                var lambda = (LambdaExpression)expression;
                return GenerateFilterName(lambda.Body);
            }
            if (expType == ExpressionType.Convert)
            {
                var unary = (UnaryExpression)expression;
                return GenerateFilterName(unary.Operand);
            }
            if (expType == ExpressionType.Constant)
            {
                var constExp = (ConstantExpression)expression;
                return constExp.Value.ToString();
            }

            throw new NotImplementedException();
        }
    }
}
