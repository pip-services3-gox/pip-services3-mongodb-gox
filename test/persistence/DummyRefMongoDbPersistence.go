package test_persistence

import (
	"context"

	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	persist "github.com/pip-services3-gox/pip-services3-mongodb-gox/persistence"
	"go.mongodb.org/mongo-driver/bson"
)

type DummyRefMongoDbPersistence struct {
	*persist.IdentifiableMongoDbPersistence[*Dummy, string]
}

func NewDummyRefMongoDbPersistence() *DummyRefMongoDbPersistence {
	c := &DummyRefMongoDbPersistence{}
	c.IdentifiableMongoDbPersistence = persist.InheritIdentifiableMongoDbPersistence[*Dummy, string](c, "dummies")
	return c
}

func (c *DummyRefMongoDbPersistence) GetPageByFilter(ctx context.Context, correlationId string, filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[*Dummy], err error) {

	filterObj := bson.M{}

	if key, ok := filter.GetAsNullableString("Key"); ok {
		filterObj = bson.M{"key": key}
	}

	sorting := bson.M{"key": -1}

	return c.IdentifiableMongoDbPersistence.GetPageByFilter(ctx, correlationId, filterObj, paging,
		sorting, nil)
}

func (c *DummyRefMongoDbPersistence) GetCountByFilter(ctx context.Context, correlationId string, filter cdata.FilterParams) (count int64, err error) {

	filterObj := bson.M{}

	if key, ok := filter.GetAsNullableString("Key"); ok {
		filterObj = bson.M{"key": key}
	}
	return c.IdentifiableMongoDbPersistence.GetCountByFilter(ctx, correlationId, filterObj)
}
