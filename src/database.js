const BatchRequestBuilder = require("unloop-batch-request")

module.exports = function(db, docClient) {
    const tableExists = function() {
        let param = {
            TableName: this.schema.TableName
        };

        return Promise.resolve().then( () =>
            new Promise((resolve, reject) => {
                db.describeTable(param, (err, data) =>
                    (err) ? reject(err) : resolve(data)
                );
            }
        ));
    }

    const createTable = async function() {
        return Promise.resolve().then( () =>
            new Promise((resolve, reject ) => {
                db.createTable(this.schema, (err, data) =>
                    err ? reject(err) : resolve(data)
                );
            }));
    }

    const initTable = async function() {
        return Promise.resolve(this.initialData.call ? this.initialData() : this.initialData)
        .then( (result) =>
            new Promise((resolve, reject ) => {
                if (!result) resolve();

                const builder = new BatchRequestBuilder();
                builder.AddItems(this.schema.TableName, result, "add");

                const param = {
                    RequestItems: builder.RenderRequest()
                }

                docClient.batchWrite(param, (err, data) =>
                    err ? reject(err) : resolve(data)
                );
            }));
    }

    const ensureTable = async function() {
        const first = tableExists.bind(this);
        const second = createTable.bind(this);
        const init = initTable.bind(this);


        await first().catch( (err) => {
            return second().then(init);
        });
    }

    const getUpdateProperties = function(entity) {
        let map = {};

        let count = 0;
        for (let prop in entity) {
            if ([this.key, this.rangeKey].indexOf(prop) >= 0) continue;
            let param = "#p" + count;
            let value = ":v" + count++;
            map[prop] = {
                param: param,
                valueParam: value,
                value: entity[prop]
            };
        }

        return map;
    }

    const getUpdateExpression = function(map) {
        let assignment = [];
        let remove = [];

        for (let prop in map) {
            if (map[prop].value !== undefined) {
                assignment.push(map[prop].param + " = " + map[prop].valueParam);
            } else {
                remove.push(map[prop].param);
            }
        }

        let operations = [];
        if (assignment.length) {
            operations.push("set " + assignment.join(", "));
        }

        if (remove.length) {
            operations.push("remove " + remove.join(", "));
        }

        return operations.length ? operations.join(require('os').EOL) : "";
    }

    const getUpdateItemInput = function(entity, partitionKey, sortKey) {
        let key = {
            [this.key]: partitionKey
        };

        if (sortKey) {
            key[this.rangeKey] = sortKey;
        }

        let result = {
            TableName: this.schema.TableName,
            Key: key
        };

        let map = getUpdateProperties.call(this, entity);
        result.UpdateExpression = getUpdateExpression.call(this,map);
        if (!result.UpdateExpression) return undefined;

        result.ExpressionAttributeValues = {};
        result.ExpressionAttributeNames = {};

        for (let prop in map) {
            result.ExpressionAttributeNames[map[prop].param] = prop;
            result.ExpressionAttributeValues[map[prop].valueParam] =
                map[prop].value;
        }

        return result;
    }

    this.getAll = async function() {
        let params = {
            TableName: this.schema.TableName
        };

        let promise = new Promise((resolve, reject) => {
            docClient.scan(params, (err, data) => {
                if (err) {
                    return reject(err);
                } else {
                    if (!data.Items || !data.Count) {
                        return resolve([]);
                    } else {
                        return resolve(data.Items);
                    }
                }
            });
        });

        return promise;
    }

    this.get = async function(partitionKey, sortKey = undefined) {
        let key = {
            [this.key]: partitionKey
        };

        if (sortKey) {
            key[this.rangeKey] = sortKey;
        }

        let params = {
            TableName: this.schema.TableName,
            Key: key
        };

        let promise = new Promise((resolve, reject) => {
            docClient.get(params, (err, data) => {
                if (err) {
                    return reject(err);
                } else {
                    if (!data.Item) {
                        return resolve(undefined);
                    } else {
                        return resolve(data.Item);
                    }
                }
            });
        });

        return promise;
    }

    this.create = async function(entity) {

        let projection = {};
        for(let prop in entity) if (entity[prop]) projection[prop] = entity[prop];

        let params = {
            TableName: this.schema.TableName,
            Item: projection
        };

        let promise = new Promise((resolve, reject) => {
            docClient.put(params, (err) =>
                err ? reject(err) : resolve(entity));
        });

        return promise;
    }

    this.batchWrite = async function(requests) {
        let params = {
            RequestItems: requests.RenderRequest()
        }

        let promise = new Promise((resolve, reject) => {
            docClient.batchWrite(params, (err) =>
                err ? reject(err) : resolve(entity));
        });

        return promise;
    }

    this.update = async function(
        entity,
        partitionKey,
        sortKey
    ) {
        let key = {
            [this.key]: partitionKey ? partitionKey : entity[this.key]
        };

        if (sortKey) {
            key[this.rangeKey] = sortKey;
        }

        let params = getUpdateItemInput.call(this,
            entity,
            partitionKey,
            sortKey
        );

        let promise = params ?
            new Promise((resolve, reject) => {

            docClient.update(
                params,
                (err) => err ? reject(err) : resolve(entity)
            );
        }) : Promise.resolve(undefined);

        return promise;
    }

    this.remove = async function(partitionKey, sortKey = undefined) {
        let key = {
            [this.key]: partitionKey
        };

        if (sortKey) {
            key[this.rangeKey] = sortKey;
        }

        let params = {
            TableName: this.schema.TableName,
            Key: key
        };

        let promise = new Promise((resolve, reject) => {
            docClient.delete(params, (err) =>
                err ? reject(err) : resolve()
            );
        });

        return promise;
    }

    const dbQuery = async function(query) {
        let params = {
            TableName: this.schema.TableName,
            KeyConditionExpression: "#key = :value",
            ExpressionAttributeNames: {
                "#key": this.key
            },
            ExpressionAttributeValues: {
                ":value": query.partitionKey
            }
        };

        if (query.isOrdered !== undefined)
            params["ScanIndexForward"] = query.isOrdered;
        if (query.max) params["Limit"] = query.max;

        let promise = new Promise((resolve, reject) => {
            docClient.query(params, (err, data) => {
                if (err) {
                    reject(err);
                } else {
                    if (!data.Items || !data.Count) {
                        resolve([]);
                    } else {
                        resolve(data.Items);
                    }
                }
            });
        });

        return promise;
    }

    const inMemoryQuery = async function(query) {
        let params = {
            TableName: this.schema.TableName
        };

        let promise = new Promise((resolve, reject) => {
            docClient.scan(params, (err, data) => {
                if (err) {
                    reject(err);
                } else {
                    if (!data.Items || !data.Count) {
                        resolve([]);
                    } else {
                        let result = data.Items
                        .sort( (first, second) => {
                            var result = 0;
                            if ( first[query.orderby] > second[query.orderby]) result = 1;
                            if ( second[query.orderby] > first[query.orderby]) result = -1;

                            if (query.isOrdered === false) result *= -1;

                            return result;
                        });

                        if (query.limit) result = result.slice(0, query.max);

                        resolve(result);
                    }
                }
            });
        });

        return promise;
    }

    this.query = async function(query) {
        return await query.partitionKey ? dbQuery.call(this, query) : inMemoryQuery.call(this, query);
    }

    this.safeOp = function(table, asyncCallback) {
        return async function() {
            await ensureTable.call(table);
            return await asyncCallback.apply(table, arguments);
        }
    }

}
