# Presto OBiBa [![Build Status](http://ci.obiba.org/view/OBiBa%20Commons/job/presto-obiba/badge/icon)](http://ci.obiba.org/view/OBiBa%20Commons/job/presto-obiba/)

[Presto](https://prestodb.io/) connectors implementation based on [OBiBa software](http://www.obiba.org). 
The implementation of these connectors is based on the REST APIs of these software. Demo is accessible at [https://presto-demo.obiba.org](https://presto-demo.obiba.org).

## Presto Opal

Presto connector over a [Opal](http://www.obiba.org/pages/products/opal/) server.

This connector provides different types of catalogs:
* catalog of values: provides for each `project` a schema and for each `table` a SQL table of individual values,
* catalog of variables: provides for each `project` a schema and for each `table` a SQL table of variables,
* catalog of system information: provides a `system` schema with SQL tables describing `database`, `plugin`, `taxonomy`, `vocabulary` and `term` objects.

### Configuration

Basic configuration is as follow:

```
connector.name=opal
opal.catalog-type=values
opal.url=http://localhost:8080/
opal.username=administrator
opal.password=password
```
The configuration keys are:

| Key               | Description   |
| ----------------- | ------------- |
| opal.url          | Opal server URL |
| opal.username     | Opal username, preferably with read only access rights |
| opal.password     | Opal username password |
| opal.catalog-type | Type of catalog: `values`, `variables` or `administration`. Optional, default is `values`  |
| opal.cache-delay  | Opal meta-data are cached during the specified delay (in seconds). Optional, default is `300` (5 minutes) |

Note that the meta-data names are normalized to fit Presto naming scheme: lower case, reserved characters etc. Despite this normalization, the connector ensures that there is no name conflict by appending an incremental number `_<n>`.
