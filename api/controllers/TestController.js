/**
 * TestControllerController
 *
 * @description :: Server-side logic for managing testcontrollers
 * @help        :: See http://links.sailsjs.org/docs/controllers
 */
var bq = require('bigquery')
  , fs = require('fs')
  , prjId = 'avail-wim'; //you need to modify this

bq.init({
  client_secret: 'availwim.json',
  key_pem: 'availwim.pem'
});


module.exports = {
	getWimStationData:function(req,res){
 		console.log('getWimStationData');
 		if(typeof req.param('id') == 'undefined'){
 			res.send('{status:"error",message:"station_id required"}',500);
 			return;
 		}
 		var station_id = req.param('id'),
 			depth = req.param('depth'),
 			database = req.param('database');

 		var select = {
 			1: 'year',
 			2: 'month',
 			3: 'day',
 			4: 'hour'
 		};

 		var SQL = generateSQL();
 		console.time('getWimStationDataQuery')
 		console.log("wimstation ",SQL)

 		//d is the value to be returned

 		bq.job.query(prjId, SQL, function(e,r,d){
 		  if(e) console.log("This is your error! It was made for you! ",e);
 		  console.timeEnd('getWimStationDataQuery')
		  res.json(d)
		});
 	// 	var request = bigQuery.jobs.query({
	 //    	kind: "bigquery#queryRequest",
	 //    	projectId: 'avail-wim',
	 //    	timeoutMs: '10000',
	 //    	resource: {query:SQL,projectId:'avail-wim'},
	 //    	auth: jwt
	 //    },

		// function(err, response) {
  //     		if (err) console.log('Error:',err);
  //     		console.timeEnd('getWimStationDataQuery')
  //     		console.time('getWimStationDataSend')
  //     		res.json(response)
  //     		console.timeEnd('getWimStationDataSend')
	 //    });
 		function generateSQL() {
 			var sql	= "SELECT " + select[depth.length] + ", class, total_weight AS weight, count(*) AS amount "
 				+ "FROM [tmasWIM12."+database+"] "
 				+ "WHERE station_id = '"+station_id+"' "
 				+ addPredicates()
 				+ "GROUP BY " + select[depth.length] + ", class, weight "
 				+ "ORDER BY " + select[depth.length] + ";";
 			return sql;
 		}
 		function addPredicates() {
 			var preds = '';
 			for (var i = 1; i < depth.length; i++) {
 				preds += 'AND ' + select[i] + ' = ' + depth[i] + ' ';
 			}
 			return preds;
 		}
	},

	/**
   * Overrides for the settings in `config/controllers.js`
   * (specific to StationsController)
   */
  _config: {}
};

