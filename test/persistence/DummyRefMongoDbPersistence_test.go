package test_persistence

import (
	"context"
	"os"
	"testing"

	cconf "github.com/pip-services3-gox/pip-services3-commons-gox/config"
)

func TestDummyRefMongoDbPersistence(t *testing.T) {

	var persistence *DummyRefMongoDbPersistence
	var fixture DummyRefPersistenceFixture

	mongoUri := os.Getenv("MONGO_URI")
	mongoHost := os.Getenv("MONGO_HOST")
	if mongoHost == "" {
		mongoHost = "localhost"
	}
	mongoPort := os.Getenv("MONGO_PORT")
	if mongoPort == "" {
		mongoPort = "27017"
	}
	mongoDatabase := os.Getenv("MONGO_DB")
	if mongoDatabase == "" {
		mongoDatabase = "test"
	}
	if mongoUri == "" && mongoHost == "" {
		return
	}

	dbConfig := cconf.NewConfigParamsFromTuples(
		"connection.uri", mongoUri,
		"connection.host", mongoHost,
		"connection.port", mongoPort,
		"connection.database", mongoDatabase,
	)

	persistence = NewDummyRefMongoDbPersistence()
	persistence.Configure(context.Background(), dbConfig)

	fixture = *NewDummyRefPersistenceFixture(persistence)

	opnErr := persistence.Open(context.Background(), "")
	if opnErr != nil {
		t.Error("Error opened persistence", opnErr)
		return
	}
	defer persistence.Close(context.Background(), "")

	opnErr = persistence.Clear(context.Background(), "")
	if opnErr != nil {
		t.Error("Error cleaned persistence", opnErr)
		return
	}

	t.Run("DummyRefMongoDbPersistence:CRUD", fixture.TestCrudOperations)
	t.Run("DummyRefMongoDbPersistence:Batch", fixture.TestBatchOperations)

}
