package test_persistence

import (
	"context"
	"reflect"

	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	persist "github.com/pip-services3-gox/pip-services3-mongodb-gox/persistence"
	"go.mongodb.org/mongo-driver/bson"
)

type DummyMapMongoDbPersistence struct {
	persist.IdentifiableMongoDbPersistence[map[string]any, string]
}

func NewDummyMapMongoDbPersistence() *DummyMapMongoDbPersistence {
	var t map[string]any
	proto := reflect.TypeOf(t)

	c := &DummyMapMongoDbPersistence{}
	c.IdentifiableMongoDbPersistence = *persist.InheritIdentifiableMongoDbPersistence[map[string]any, string](c, proto, "dummies")
	return c
}

func (c *DummyMapMongoDbPersistence) GetPageByFilter(ctx context.Context, correlationId string, filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[map[string]any], err error) {

	filterObj := bson.M{}

	if key, ok := filter.GetAsNullableString("Key"); ok {
		filterObj = bson.M{"key": key}
	}

	sorting := bson.M{"key": -1}

	return c.IdentifiableMongoDbPersistence.GetPageByFilter(ctx, correlationId, filterObj, paging,
		sorting, nil)
}

func (c *DummyMapMongoDbPersistence) GetCountByFilter(ctx context.Context, correlationId string, filter cdata.FilterParams) (count int64, err error) {

	filterObj := bson.M{}

	if key, ok := filter.GetAsNullableString("Key"); ok {
		filterObj = bson.M{"key": key}
	}
	return c.IdentifiableMongoDbPersistence.GetCountByFilter(ctx, correlationId, filterObj)
}
