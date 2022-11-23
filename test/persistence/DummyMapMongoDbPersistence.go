package test_persistence

import (
	"context"

	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	persist "github.com/pip-services3-gox/pip-services3-mongodb-gox/persistence"
	"go.mongodb.org/mongo-driver/bson"
)

type DummyMapMongoDbPersistence struct {
	*persist.IdentifiableMongoDbPersistence[map[string]any, string]
}

func NewDummyMapMongoDbPersistence() *DummyMapMongoDbPersistence {
	c := &DummyMapMongoDbPersistence{}
	c.IdentifiableMongoDbPersistence = persist.InheritIdentifiableMongoDbPersistence[map[string]any, string](c, "dummies")
	return c
}

func (c *DummyMapMongoDbPersistence) GetPageByFilter(ctx context.Context, correlationId string, filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[map[string]any], err error) {

	filterObj := bson.M{}

	if key, ok := filter.GetAsNullableString("Key"); ok {
		filterObj = bson.M{"Key": key}
	}

	sorting := bson.M{"Key": -1}

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
