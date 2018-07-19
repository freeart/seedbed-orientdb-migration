const assert = require('assert'),
	async = require('async')

module.exports = function () {
	assert(this.orientDB, "field not exists");

	const migrationLog = require(this.config.get("migration.file"));

	return new Promise((resolve, reject) => {
		async.autoInject({
			schema: (cb) => {
				this.orientDB.query(`
					select name, customFields.version as version from (select expand(classes) from metadata:schema)
				`).then(
					(rows) => {
						const index = {};
						for (let i = 0; i < rows.length; i++) {
							index[rows[i].name] = rows[i];
						}
						cb(null, index);
					}).catch((err) => cb(err))
			},
			toApply: (schema, cb) => {
				const migrationUpdate = {};
				for (const [table, timelog] of Object.entries(migrationLog)) {
					for (const [timestamp, sqlLog] of Object.entries(timelog)) {
						migrationUpdate[table] = migrationUpdate[table] || {};
						if (timestamp > schema[table]) {
							migrationUpdate[table][timestamp] = sqlLog;
						}
					}
				}
				cb(null, migrationUpdate)
			},
			migrate: (toApply, cb) => {
				async.eachOfSeries(toApply, (timelog, table, cb) => {
					async.eachOfSeries(timelog, (sqls, timestamp, cb) => {
						sqls.push(`ALTER CLASS ${table} CUSTOM version=${timestamp}`);
						async.eachSeries(sqls, (sql, cb) => {
							this.orientDB.query(`ALTER CLASS ${table} CUSTOM version=:version`).then((res) => cb(null, res)).catch((err) => cb(err));
						});
					});
				}, (err) => cb(err));
			}
		}, (err) => {
			if (err) {
				return reject(err);
			}
			resolve();
		})
	});

	// const timestamp = moment().utc().format('x');
}