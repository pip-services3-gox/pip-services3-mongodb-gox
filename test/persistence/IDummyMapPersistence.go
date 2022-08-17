package test_persistence

import (
	"context"
	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
)

type IDummyMapPersistence interface {
	GetPageByFilter(ctx context.Context, correlationId string, filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[map[string]any], err error)
	GetListByIds(ctx context.Context, correlationId string, ids []string) (items []map[string]any, err error)
	GetOneById(ctx context.Context, correlationId string, id string) (item map[string]any, err error)
	Create(ctx context.Context, correlationId string, item map[string]any) (result map[string]any, err error)
	Update(ctx context.Context, correlationId string, item map[string]any) (result map[string]any, err error)
	UpdatePartially(ctx context.Context, correlationId string, id string, data cdata.AnyValueMap) (item map[string]any, err error)
	DeleteById(ctx context.Context, correlationId string, id string) (item map[string]any, err error)
	DeleteByIds(ctx context.Context, correlationId string, ids []string) (err error)
	GetCountByFilter(ctx context.Context, correlationId string, filter cdata.FilterParams) (count int64, err error)
}
