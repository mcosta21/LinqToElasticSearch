using System;
using System.Collections.Generic;
using System.Linq;
using AutoFixture;
using FluentAssertions;
using Xunit;

namespace LinqToElasticSearch.IntegrationTests.Clauses.WhereByTypes
{
    public class WhereMixTests: IntegrationTestsBase<SampleData>
    {
       
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WhereTwice(bool together)
        {
            //Given
            var datas = Fixture.CreateMany<SampleData>().ToList();

            datas[1].Name = "123456789";
            datas[1].Age = 23;
            datas[2].Name = "123456789";
            
            Bulk(datas);

            ElasticClient.Indices.Refresh();
            
            //When
            IQueryable<SampleData> results;
            if (together)
            {
                results = Sut.Where(x => x.Name.Contains("4567") && x.Age == 23);
            }
            else
            {
                results = Sut.Where(x => x.Name.Contains("4567"));
                results = results.Where(x => x.Age == 23);
            }
            var listResults = results.ToList();

            //Then
            listResults.Count.Should().Be(1);
            listResults[0].Name.Should().Be(datas[1].Name);
        }

        [Fact]
        public void WhereWithTwoConditionsUsingContaingAndExtrinsicComparation()
        {
            var allowedFolders = new List<Guid>
            {
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>()
            };
            
            var allowedTypes = new List<Guid>
            {
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>()
            };
            
            var items = Fixture.CreateMany<SampleData>(4).ToList();
            items[0].FolderId = allowedFolders[0];
            items[0].TypeId = allowedTypes[0];
            items[1].FolderId = allowedFolders[2];
            items[2].FolderId = null;
            items[2].TypeId = allowedTypes[1];
            items[3].FolderId = null;

            var canReadWithoutFolder = false;
            
            Bulk(items);
            ElasticClient.Indices.Refresh();

            var results = Sut.Where(item =>
                (item.FolderId == null && (canReadWithoutFolder || allowedTypes.Contains(item.TypeId)))
                || (item.FolderId != null && allowedFolders.Contains(item.FolderId.Value)));

            results.Should().HaveCount(3);
        }
        
        [Fact]
        public void WhereWithTwoConditionsUsingContaingAndIntrinsicComparation()
        {
            var allowedFolders = new List<Guid>
            {
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>()
            };
            
            var allowedTypes = new List<Guid>
            {
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>()
            };
            
            var items = Fixture.CreateMany<SampleData>(4).ToList();
            items[0].FolderId = allowedFolders[0];
            items[0].TypeId = allowedTypes[0];
            items[1].FolderId = allowedFolders[2];
            items[2].FolderId = null;
            items[2].TypeId = allowedTypes[1];
            items[3].FolderId = null;

            var canReadWithoutFolder = false;
            
            Bulk(items);
            ElasticClient.Indices.Refresh();

            var results = Sut.Where(item =>
                (item.FolderId == null && (allowedTypes.Contains(item.TypeId))) == true
                || (item.FolderId != null && allowedFolders.Contains(item.FolderId.Value)) == true);

            results.Should().HaveCount(3);
            results.FirstOrDefault(x => x.Id.Value == items[0].Id.Value).Should().NotBeNull();
            results.FirstOrDefault(x => x.Id.Value == items[1].Id.Value).Should().NotBeNull();
            results.FirstOrDefault(x => x.Id.Value == items[2].Id.Value).Should().NotBeNull();
            results.FirstOrDefault(x => x.Id.Value == items[3].Id.Value).Should().BeNull();
        }
        
        [Fact]
        public void WhereWithTwoConditionsUsingContaingAndIntrinsicComparation2()
        {
            var allowedFolders = new List<Guid>
            {
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>()
            };
            
            var allowedTypes = new List<Guid>
            {
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>()
            };
            
            var items = Fixture.CreateMany<SampleData>(4).ToList();
            items[0].FolderId = allowedFolders[0];
            items[0].TypeId = allowedTypes[0];
            items[1].FolderId = allowedFolders[2];
            items[2].FolderId = null;
            items[2].TypeId = allowedTypes[1];
            items[3].FolderId = null;

            var canReadWithoutFolder = false;
            
            Bulk(items);
            ElasticClient.Indices.Refresh();

            var results = Sut.Where(item =>
                (item.FolderId == null) == true);

            results.Should().HaveCount(2);
            results.FirstOrDefault(x => x.Id.Value == items[0].Id.Value).Should().BeNull();
            results.FirstOrDefault(x => x.Id.Value == items[1].Id.Value).Should().BeNull();
            results.FirstOrDefault(x => x.Id.Value == items[2].Id.Value).Should().NotBeNull();
            results.FirstOrDefault(x => x.Id.Value == items[3].Id.Value).Should().NotBeNull();
        }
        
        [Fact]
        public void WhereWithTwoConditionsUsingContaingAndIntrinsicComparation3()
        {
            var allowedFolders = new List<Guid>
            {
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>()
            };
            
            var allowedTypes = new List<Guid>
            {
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>()
            };
            
            var items = Fixture.CreateMany<SampleData>(4).ToList();
            items[0].FolderId = allowedFolders[0];
            items[0].TypeId = allowedTypes[0];
            items[1].FolderId = allowedFolders[2];
            items[2].FolderId = null;
            items[2].TypeId = allowedTypes[1];
            items[3].FolderId = null;

            var canReadWithoutFolder = false;
            
            Bulk(items);
            ElasticClient.Indices.Refresh();

            var results = Sut.Where(item =>
                (item.FolderId == null && (canReadWithoutFolder || allowedTypes.Contains(item.TypeId))) == true);

            results.Should().HaveCount(1);
            results.FirstOrDefault(x => x.Id.Value == items[0].Id.Value).Should().BeNull();
            results.FirstOrDefault(x => x.Id.Value == items[1].Id.Value).Should().BeNull();
            results.FirstOrDefault(x => x.Id.Value == items[2].Id.Value).Should().NotBeNull();
            results.FirstOrDefault(x => x.Id.Value == items[3].Id.Value).Should().BeNull();
        }
        
        [Fact]
        public void WhereWithTwoConditionsUsingContaingAndIntrinsicComparation4()
        {
            var allowedFolders = new List<Guid>
            {
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>()
            };
            
            var allowedTypes = new List<Guid>
            {
                Fixture.Create<Guid>(),
                Fixture.Create<Guid>()
            };
            
            var items = Fixture.CreateMany<SampleData>(4).ToList();
            items[0].FolderId = allowedFolders[0];
            items[0].TypeId = allowedTypes[0];
            items[1].FolderId = allowedFolders[2];
            items[2].FolderId = null;
            items[2].TypeId = allowedTypes[1];
            items[3].FolderId = null;

            var canReadWithoutFolder = false;
            
            Bulk(items);
            ElasticClient.Indices.Refresh();

            var results = Sut.Where(item =>
                (item.FolderId != null && allowedFolders.Contains(item.FolderId.Value)) == true);

            results.Should().HaveCount(2);
            results.FirstOrDefault(x => x.Id.Value == items[0].Id.Value).Should().NotBeNull();
            results.FirstOrDefault(x => x.Id.Value == items[1].Id.Value).Should().NotBeNull();
            results.FirstOrDefault(x => x.Id.Value == items[2].Id.Value).Should().BeNull();
            results.FirstOrDefault(x => x.Id.Value == items[3].Id.Value).Should().BeNull();
        }
        
    }
}