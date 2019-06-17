const database = require('./src/database');

module.exports = (db, docClient) => {

    const instance = new database(db, docClient);    

    return function(table) {
        const safeOp = (command) => instance.safeOp(instance.command(command));
        const sanitizeOutputChain = (command) => instance.sanitizeOutput(safeOp(command))(table);
        const sanitizeBothChain =  (command) => instance.sanitizeOutput(instance.sanitizeInput(safeOp(command)))(table);
        const noSanitizeChain = (command) => safeOp(command)(table);

        this.getAll = sanitizeOutputChain(instance.getAll);
        this.unsafeGetAll = noSanitizeChain(instance.getAll);
        this.get = sanitizeOutputChain(instance.get);
        this.unsafeGet = noSanitizeChain(instance.get);
        this.create = sanitizeOutputChain(instance.create);
        this.update = sanitizeBothChain(instance.update);
        this.unsafeUpdate = noSanitizeChain(instance.update);
        this.delete = noSanitizeChain(instance.remove);
        this.query = sanitizeOutputChain(instance.query);
        this.batch = instance.batchWrite;
    }
}