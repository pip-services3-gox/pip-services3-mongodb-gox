# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> MongoDB components for Golang

This module is a part of the [Pip.Services](http://pipservices.org) polyglot microservices toolkit. It provides a set of components to implement MongoDB persistence.

Client was based on [official mongodb go driver](https://github.com/mongodb/mongo-go-driver)
[Official docs](https://docs.mongodb.com/ecosystem/drivers/go/) for MongoDb Go driver

The module contains the following packages:
- [**Build**](https://godoc.org/github.com/pip-services3-gox/pip-services3-mongodb-gox/build) -  Factory to create MongoDB persistence components.
- [**Connect**](https://godoc.org/github.com/pip-services3-gox/pip-services3-mongodb-gox/connect) - Connection component to configure MongoDB connection to database.
- [**Persistence**](https://godoc.org/github.com/pip-services3-gox/pip-services3-mongodb-gox/persistence) - abstract persistence components to perform basic CRUD operations.

<a name="links"></a> Quick links:

* [MongoDB persistence](https://www.pipservices.org/recipies/mongodb-persistence)
* [Configuration](https://www.pipservices.org/recipies/configuration)
* [API Reference](https://godoc.org/github.com/pip-services3-gox/pip-services3-mongodb-gox/)
* [Change Log](CHANGELOG.md)
* [Get Help](https://www.pipservices.org/community/help)
* [Contribute](https://www.pipservices.org/community/contribute)

## Use

Get the package from the Github repository:
```bash
go get -u github.com/pip-services3-gox/pip-services3-mongodb-gox@latest
```

## Develop

For development you shall install the following prerequisites:
* Golang v1.18+
* Visual Studio Code or another IDE of your choice
* Docker
* Git

Run automated tests:
```bash
go test -v ./test/...
```

Generate API documentation:
```bash
./docgen.ps1
```

Before committing changes run dockerized test as:
```bash
./test.ps1
./clear.ps1
```

## Contacts

The Golang version of Pip.Services is created and maintained by:
- **Levichev Dmitry**
- **Sergey Seroukhov**
- **Dmitry Uzdemir**

The documentation is written by:
- **Levichev Dmitry**
