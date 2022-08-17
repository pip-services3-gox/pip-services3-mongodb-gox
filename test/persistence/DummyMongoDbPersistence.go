package test_persistence

import (
	"context"
	"reflect"

	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	persist "github.com/pip-services3-gox/pip-services3-mongodb-gox/persistence"
	"go.mongodb.org/mongo-driver/bson"
)

type DummyMongoDbPersistence struct {
	persist.IdentifiableMongoDbPersistence[Dummy, string]
}

func NewDummyMongoDbPersistence() *DummyMongoDbPersistence {
	proto := reflect.TypeOf(Dummy{})
	c := &DummyMongoDbPersistence{}
	c.IdentifiableMongoDbPersistence = *persist.InheritIdentifiableMongoDbPersistence[Dummy, string](c, proto, "dummies")
	return c
}

func (c *DummyMongoDbPersistence) GetPageByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[Dummy], err error) {

	filterObj := bson.M{}

	if key, ok := filter.GetAsNullableString("Key"); ok {
		filterObj = bson.M{"key": key}
	}

	sorting := bson.M{"key": -1}

	return c.IdentifiableMongoDbPersistence.GetPageByFilter(ctx, correlationId,
		filterObj, paging,
		sorting, nil)
}

func (c *DummyMongoDbPersistence) GetCountByFilter(ctx context.Context, correlationId string, filter cdata.FilterParams) (count int64, err error) {

	filterObj := bson.M{}

	if key, ok := filter.GetAsNullableString("Key"); ok {
		filterObj = bson.M{"key": key}
	}

	return c.IdentifiableMongoDbPersistence.GetCountByFilter(ctx, correlationId, filterObj)
}
