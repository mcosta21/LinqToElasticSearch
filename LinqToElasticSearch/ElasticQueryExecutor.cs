﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using Nest;
using Newtonsoft.Json;
using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.ResultOperators;

namespace LinqToElasticSearch
{
    public class ElasticQueryExecutor<TK>: IQueryExecutor
    {
        private readonly IElasticClient _elasticClient;
        private readonly string _dataId;
        private readonly PropertyNameInferrerParser _propertyNameInferrerParser;
        private readonly ElasticGeneratorQueryModelVisitor<TK> _elasticGeneratorQueryModelVisitor;

        public ElasticQueryExecutor(IElasticClient elasticClient, string dataId)
        {
            _elasticClient = elasticClient;
            _dataId = dataId;
            _propertyNameInferrerParser = new PropertyNameInferrerParser(_elasticClient);
            _elasticGeneratorQueryModelVisitor = new ElasticGeneratorQueryModelVisitor<TK>(_propertyNameInferrerParser);
        }

        public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
        {
            var queryAggregator = _elasticGeneratorQueryModelVisitor.GenerateElasticQuery<T>(queryModel);

            var documents= _elasticClient.Search<IDictionary<string, object>>(descriptor =>
            {
                descriptor.Index(_dataId);

                if (queryModel.SelectClause != null && queryModel.SelectClause.Selector is MemberExpression memberExpression)
                {
                    descriptor.Source(x => x.Includes(f => f.Field(_propertyNameInferrerParser.Parser(memberExpression.Member.Name))));
                }

                if (queryAggregator.Skip != null)
                {
                    descriptor.From(queryAggregator.Skip);
                }
                else
                {
                    descriptor.Size(10000);
                }

                if (queryAggregator.Take != null)
                {
                    descriptor.Take(queryAggregator.Take);
                    descriptor.Size(queryAggregator.Take);
                }
                
                if (queryAggregator.QueryContainers.Any())
                {
                    descriptor.Query(q => q.Bool(x => x.Must(queryAggregator.QueryContainers.ToArray())));
                }
                else
                {
                    descriptor.MatchAll();
                }


                if (queryAggregator.OrderByExpressions.Any())
                {
                    descriptor.Sort(d =>
                    {
                        foreach (var orderByExpression in queryAggregator.OrderByExpressions)
                        {
                            var property = _propertyNameInferrerParser.Parser(orderByExpression.PropertyName) +
                                           orderByExpression.GetKeywordIfNecessary();
                            d.Field(property,
                                orderByExpression.OrderingDirection == OrderingDirection.Asc
                                    ? SortOrder.Ascending
                                    : SortOrder.Descending);
                        }

                        return d;
                    });
                }

                if (queryAggregator.GroupByExpressions.Any())
                {
                    descriptor.Aggregations(a =>
                    {

                        a.Composite("composite", c =>
                                c.Sources(so =>
                                {
                                    queryAggregator.GroupByExpressions.ForEach(gbe =>
                                    {
                                        var property = _propertyNameInferrerParser.Parser(gbe.PropertyName) + gbe.GetKeywordIfNecessary();
                                        so.Terms($"group_by_{gbe.PropertyName}", t => t.Field(property));
                                    });

                                    return so;
                                })
                                .Aggregations(aa => aa
                                    .TopHits("data_composite", th => th)   
                                )
                            );

                        return a;
                    });

                }
                
                return descriptor;

            });
            
            if (queryModel.SelectClause?.Selector is MemberExpression)
            {
                return JsonConvert.DeserializeObject<IEnumerable<T>>(
                    JsonConvert.SerializeObject(documents.Documents.SelectMany(x => x.Values), Formatting.Indented));
            }

            if (queryAggregator.GroupByExpressions.Any())
            {
                var docDeserializer = new Func<object, TK>(input => 
                    JsonConvert.DeserializeObject<TK>(JsonConvert.SerializeObject(input, Formatting.Indented)));

                var originalGroupingType = queryModel.GetResultType().GenericTypeArguments.First();
                var originalGroupingGenerics = originalGroupingType.GetGenericArguments();

                var genericListType = typeof(List<>).MakeGenericType(originalGroupingType);
                var values = (IList)Activator.CreateInstance(genericListType);
                
                var composite = documents.Aggregations.Composite("composite");
            
                foreach(var bucket in composite.Buckets)
                {
                    var key = GenerateKey(bucket.Key);
                    var list = bucket.TopHits("data_composite").Documents<object>().Select(docDeserializer).ToList();

                    var grouping = typeof(Grouping<,>);
                    var groupingGenerics = grouping.MakeGenericType(originalGroupingGenerics);
                    var groupingInstance = Activator.CreateInstance(groupingGenerics, key, list);
                    values.Add(groupingInstance);
                }
                
                return values.Cast<T>();
            }

            var result = JsonConvert.DeserializeObject<IEnumerable<T>>(
                JsonConvert.SerializeObject(documents.Documents, Formatting.Indented));

            return result;
        }

        public T ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty)
        {
            var sequence = ExecuteCollection<T>(queryModel);

            return returnDefaultWhenEmpty ? sequence.SingleOrDefault() : sequence.Single();
        }

        public T ExecuteScalar<T>(QueryModel queryModel) 
        {
            var queryAggregator = _elasticGeneratorQueryModelVisitor.GenerateElasticQuery<T>(queryModel);

            foreach (var resultOperator in queryModel.ResultOperators)
            {
                if (resultOperator is CountResultOperator)
                {
                    var result = _elasticClient.Count<object>(descriptor =>
                    {
                        descriptor.Index(_dataId);
                    
                        if (queryAggregator.QueryContainers.Any())
                        {
                            descriptor.Query(q => q.Bool(x => x.Must(queryAggregator.QueryContainers.ToArray())));
                        }
                        return descriptor;
                    }).Count;
            
                    var converter = TypeDescriptor.GetConverter(typeof(T));
                    if (converter.CanConvertFrom(typeof(string)))
                    {
                        return (T)converter.ConvertFromString(result.ToString());
                    }
                }
            }
            
            return default(T);
        }

        private dynamic GenerateKey(CompositeKey ck)
        {
            IDictionary<string, object> expando = new ExpandoObject();
            foreach (var entry in ck)
            {
                var key = entry.Key.Replace("group_by_", "");
                expando[key] = entry.Value;
            }

            return expando;
        }
    }

    public class Grouping<TKey, TElem> : IGrouping<TKey, TElem>
    {
        public TKey Key { get; }
        
        private readonly IEnumerable<TElem> _values;

        public Grouping(TKey key, IEnumerable<TElem> values)
        {
            Key = key;
            _values = values;
        }

        public IEnumerator<TElem> GetEnumerator()
        {
            return _values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}