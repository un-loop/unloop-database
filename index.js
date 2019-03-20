const database = require('./src/database');

module.exports = (db, docClient) => {

    const instance = new database(db, docClient);

    return function(table) {
        this.getAll = instance.safeOp(table, instance.getAll);
        this.get = instance.safeOp(table, instance.get);
        this.create = instance.safeOp(table, instance.create);
        this.update = instance.safeOp(table, instance.update);
        this.delete = instance.safeOp(table, instance.remove);
        this.query = instance.safeOp(table, instance.query);
        this.batch = instance.batchWrite;
    }
}