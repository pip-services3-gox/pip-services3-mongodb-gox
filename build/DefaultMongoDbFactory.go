package build

import (
	cref "github.com/pip-services3-gox/pip-services3-commons-gox/refer"
	cbuild "github.com/pip-services3-gox/pip-services3-components-gox/build"
	conn "github.com/pip-services3-gox/pip-services3-mongodb-gox/connect"
)

// DefaultMongoDbFactory helps creates MongoDb components by their descriptors.
//
//	see Factory
//	see MongoDbConnection
type DefaultMongoDbFactory struct {
	cbuild.Factory
}

// NewDefaultMongoDbFactory are create a new instance of the factory.
//
//	Returns: *DefaultMongoDbFactory
func NewDefaultMongoDbFactory() *DefaultMongoDbFactory {
	c := DefaultMongoDbFactory{}

	mongoDbConnectionDescriptor := cref.NewDescriptor("pip-services", "connection", "mongodb", "*", "1.0")

	c.RegisterType(mongoDbConnectionDescriptor, conn.NewMongoDbConnection)
	return &c
}
