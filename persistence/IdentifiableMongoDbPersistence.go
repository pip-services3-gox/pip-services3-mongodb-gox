package persistence

import (
	"context"
	"errors"
	"reflect"

	cconf "github.com/pip-services3-gox/pip-services3-commons-gox/config"
	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	cmpersist "github.com/pip-services3-gox/pip-services3-data-gox/persistence"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mngoptions "go.mongodb.org/mongo-driver/mongo/options"
)

// IdentifiableMongoDbPersistence is abstract persistence component that stores data in MongoDB
// and implements a number of CRUD operations over data items with unique ids.
// The data items must implement IIdentifiable interface.
//
// In basic scenarios child classes shall only override GetPageByFilter,
// GetListByFilter or DeleteByFilter operations with specific filter function.
// All other operations can be used out of the box.
//
// In complex scenarios child classes can implement additional operations by
// accessing c.Collection properties.
//
//	Configuration parameters:
//		- collection:                  (optional) MongoDB collection name
//		- connection(s):
//			- discovery_key:             (optional) a key to retrieve the connection from IDiscovery
//			- host:                      host name or IP address
//			- port:                      port number (default: 27017)
//			- database:                  database name
//			- uri:                       resource URI or connection string with all parameters in it
//		- credential(s):
//			- store_key:                 (optional) a key to retrieve the credentials from ICredentialStore
//			- username:                  (optional) user name
//			- password:                  (optional) user password
//		- options:
//			- max_pool_size:             (optional) maximum connection pool size (default: 2)
//			- keep_alive:                (optional) enable connection keep alive (default: true)
//			- connect_timeout:           (optional) connection timeout in milliseconds (default: 5000)
//			- socket_timeout:            (optional) socket timeout in milliseconds (default: 360000)
//			- auto_reconnect:            (optional) enable auto reconnection (default: true) (not used)
//			- reconnect_interval:        (optional) reconnection interval in milliseconds (default: 1000) (not used)
//			- max_page_size:             (optional) maximum page size (default: 100)
//			- replica_set:               (optional) name of replica set
//			- ssl:                       (optional) enable SSL connection (default: false) (not implements in this release)
//			- auth_source:               (optional) authentication source
//			- debug:                     (optional) enable debug output (default: false). (not used)
//
//	References:
//		- *:logger:*:*:1.0           (optional) ILogger components to pass log messages components to pass log messages
//		- *:discovery:*:*:1.0        (optional) IDiscovery services
//		- *:credential-store:*:*:1.0 (optional) Credential stores to resolve credentials
//
//	Example: TODO::add valid example
type IdentifiableMongoDbPersistence[T any, K any] struct {
	MongoDbPersistence[T]
}

// InheritIdentifiableMongoDbPersistence is creates a new instance of the persistence component.
//
//	Parameters:
//		- proto reflect.Type of saved data, need for correct decode from DB
//		- collection string (optional) a collection name.
//	Returns: *IdentifiableMongoDbPersistence[T, K] new created IdentifiableMongoDbPersistence component
func InheritIdentifiableMongoDbPersistence[T any, K any](overrides IMongoDbPersistenceOverrides[T], proto reflect.Type, collection string) *IdentifiableMongoDbPersistence[T, K] {
	if collection == "" {
		panic("Collection name could not be nil")
	}
	c := IdentifiableMongoDbPersistence[T, K]{}
	c.MongoDbPersistence = *InheritMongoDbPersistence[T](overrides, proto, collection)
	c.maxPageSize = 100
	return &c
}

// Configure is configures component by passing configuration parameters.
//
//	Parameters:
//		- ctx context.Context
//		- config  *cconf.ConfigParams configuration parameters to be set.
func (c *IdentifiableMongoDbPersistence[T, K]) Configure(ctx context.Context, config *cconf.ConfigParams) {
	c.MongoDbPersistence.Configure(ctx, config)
	c.maxPageSize = (int32)(config.GetAsIntegerWithDefault("options.max_page_size", (int)(c.maxPageSize)))
}

// GetListByIds is gets a list of data items retrieved by given unique ids.
//
//	Parameters:
//		- ctx context.Context
//		- correlationId  string (optional) transaction id to Trace execution through call chain.
//		- ids  []K ids of data items to be retrieved
//	Returns: items []T, err error a data list and error, if they are occurred.
func (c *IdentifiableMongoDbPersistence[T, K]) GetListByIds(ctx context.Context, correlationId string,
	ids []K) (items []T, err error) {

	filter := bson.M{
		"_id": bson.M{"$in": ids},
	}
	return c.GetListByFilter(ctx, correlationId, filter, nil, nil)
}

// GetOneById is gets a data item by its unique id.
//
//	Parameters:
//		- ctx context.Context
//		- correlationId     (optional) transaction id to Trace execution through call chain.
//		- id                an id of data item to be retrieved.
//	Returns: item T, err error a data and error, if they are occurred.
func (c *IdentifiableMongoDbPersistence[T, K]) GetOneById(ctx context.Context, correlationId string,
	id K) (item T, err error) {

	filter := bson.M{"_id": id}
	docPointer := c.NewObjectByPrototype()

	res := c.Collection.FindOne(ctx, filter)
	if err := res.Err(); err != nil {
		return item, err
	}

	if err := res.Decode(docPointer.Interface()); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return item, nil
		}
		return item, err
	}
	c.Logger.Trace(ctx, correlationId, "Retrieved from %s by id = %s", c.CollectionName, id)
	return c.Overrides.ConvertToPublic(docPointer), nil
}

// Create was creates a data item.
//
//	Parameters:
//		- ctx context.Context
//		- correlation_id string (optional) transaction id to Trace execution through call chain.
//		- item any an item to be created.
//	Returns: result any, err error created item and error, if they are occurred
func (c *IdentifiableMongoDbPersistence[T, K]) Create(ctx context.Context, correlationId string,
	item T) (result T, err error) {

	var newItem any
	newItem = cmpersist.CloneObject(item, c.Prototype)
	// Assign unique id if not exist
	cmpersist.GenerateObjectId(&newItem)
	newItem = c.Overrides.ConvertFromPublic(newItem)
	res, err := c.Collection.InsertOne(ctx, newItem)
	if err != nil {
		return result, err
	}

	result = c.Overrides.ConvertToPublic(newItem)

	c.Logger.Trace(ctx, correlationId, "Created in %s with id = %s", c.Collection, res.InsertedID)

	return result, nil
}

// Set is sets a data item. If the data item exists it updates it,
// otherwise it create a new data item.
//
//	Parameters:
//		- ctx context.Context
//		- correlation_id string (optional) transaction id to Trace execution through call chain.
//		- item T an item to be set.
//	Returns: result any, err error updated item and error, if they occurred
func (c *IdentifiableMongoDbPersistence[T, K]) Set(ctx context.Context, correlationId string,
	item T) (result T, err error) {

	var newItem any
	newItem = cmpersist.CloneObject(item, c.Prototype)
	// Assign unique id if not exist
	cmpersist.GenerateObjectId(&newItem)
	id := cmpersist.GetObjectId(newItem)
	c.Overrides.ConvertFromPublic(newItem)

	filter := bson.M{"_id": id}
	var options mngoptions.FindOneAndReplaceOptions
	retDoc := mngoptions.After
	options.ReturnDocument = &retDoc
	upsert := true
	options.Upsert = &upsert

	res := c.Collection.FindOneAndReplace(ctx, filter, newItem, &options)
	if err := res.Err(); err != nil {
		return result, err
	}

	c.Logger.Trace(ctx, correlationId, "Set in %s with id = %s", c.CollectionName, id)
	docPointer := c.NewObjectByPrototype()
	if err := res.Decode(docPointer.Interface()); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return result, nil
		}
		return result, err
	}

	return c.Overrides.ConvertToPublic(docPointer), nil
}

// Update is updates a data item.
//
//	Parameters:
//		- ctx context.Context
//		- correlation_id string (optional) transaction id to Trace execution through call chain.
//		- item T an item to be updated.
//	Returns: result any, err error updated item and error, if they are occurred
func (c *IdentifiableMongoDbPersistence[T, K]) Update(ctx context.Context, correlationId string,
	item T) (result T, err error) {

	newItem := cmpersist.CloneObject(item, c.Prototype)
	id := cmpersist.GetObjectId(newItem)

	filter := bson.M{"_id": id}
	update := bson.D{{"$set", newItem}}

	var options mngoptions.FindOneAndUpdateOptions
	retDoc := mngoptions.After
	options.ReturnDocument = &retDoc

	res := c.Collection.FindOneAndUpdate(ctx, filter, update, &options)
	if err := res.Err(); err != nil {
		return result, err
	}

	c.Logger.Trace(ctx, correlationId, "Updated in %s with id = %s", c.CollectionName, id)

	docPointer := c.NewObjectByPrototype()
	if err := res.Decode(docPointer.Interface()); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return result, nil
		}
		return result, err
	}

	return c.Overrides.ConvertToPublic(docPointer), nil
}

// UpdatePartially is updates only few selected fields in a data item.
//
//	Parameters:
//		- ctx context.Context
//		- correlation_id string (optional) transaction id to Trace execution through call chain.
//		- id K an id of data item to be updated.
//		- data cdata.AnyValueMap a map with fields to be updated.
//	Returns: item any, err error updated item and error, if they are occurred
func (c *IdentifiableMongoDbPersistence[T, K]) UpdatePartially(ctx context.Context, correlationId string,
	id K, data cdata.AnyValueMap) (item T, err error) {

	newItem := bson.M{}
	for k, v := range data.Value() {
		newItem[k] = v
	}
	filter := bson.M{"_id": id}
	update := bson.D{{"$set", newItem}}

	var options mngoptions.FindOneAndUpdateOptions
	retDoc := mngoptions.After
	options.ReturnDocument = &retDoc

	res := c.Collection.FindOneAndUpdate(ctx, filter, update, &options)
	if err := res.Err(); err != nil {
		return item, err
	}
	c.Logger.Trace(ctx, correlationId, "Updated partially in %s with id = %s", c.Collection, id)

	docPointer := c.NewObjectByPrototype()
	if err := res.Decode(docPointer.Interface()); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return item, nil
		}
		return item, err
	}

	return c.Overrides.ConvertToPublic(docPointer), nil
}

// DeleteById is deleted a data item by it's unique id.
//
//	Parameters:
//		- ctx context.Context
//		- correlation_id string (optional) transaction id to Trace execution through call chain.
//		- id K id of the item to be deleted
//	Returns: item T, err error deleted item and error, if they are occurred
func (c *IdentifiableMongoDbPersistence[T, K]) DeleteById(ctx context.Context, correlationId string,
	id K) (item T, err error) {

	filter := bson.M{"_id": id}

	res := c.Collection.FindOneAndDelete(ctx, filter)
	if err := res.Err(); err != nil {
		return item, err
	}

	c.Logger.Trace(ctx, correlationId, "Deleted from %s with id = %s", c.CollectionName, id)

	docPointer := c.NewObjectByPrototype()
	if err := res.Decode(docPointer.Interface()); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return item, nil
		}
		return item, err
	}

	return c.Overrides.ConvertToPublic(docPointer), nil
}

// DeleteByIds is deletes multiple data items by their unique ids.
//
//	Parameters:
//		- ctx context.Context
//		- correlationId string (optional) transaction id to Trace execution through call chain.
//		- ids []K ids of data items to be deleted.
//	Returns: error or nil for success.
func (c *IdentifiableMongoDbPersistence[T, K]) DeleteByIds(ctx context.Context, correlationId string,
	ids []K) error {

	filter := bson.M{
		"_id": bson.M{"$in": ids},
	}
	return c.DeleteByFilter(ctx, correlationId, filter)
}
