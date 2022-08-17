package test_persistence

import (
	"context"
	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
)

type IDummyPersistence interface {
	GetPageByFilter(ctx context.Context, correlationId string, filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[Dummy], err error)
	GetListByIds(ctx context.Context, correlationId string, ids []string) (items []Dummy, err error)
	GetOneById(ctx context.Context, correlationId string, id string) (item Dummy, err error)
	Create(ctx context.Context, correlationId string, item Dummy) (result Dummy, err error)
	Update(ctx context.Context, correlationId string, item Dummy) (result Dummy, err error)
	UpdatePartially(ctx context.Context, correlationId string, id string, data cdata.AnyValueMap) (item Dummy, err error)
	DeleteById(ctx context.Context, correlationId string, id string) (item Dummy, err error)
	DeleteByIds(ctx context.Context, correlationId string, ids []string) (err error)
	GetCountByFilter(ctx context.Context, correlationId string, filter cdata.FilterParams) (count int64, err error)
}
