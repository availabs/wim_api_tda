/**
 * StationsController
 *
 * @module      :: Controller
 * @description	:: A set of functions called `actions`.
 *
 *                 Actions contain code telling Sails how to respond to a certain type of request.
 *                 (i.e. do stuff, then send some JSON, show an HTML page, or redirect to another URL)
 *
 *                 You can configure the blueprint URLs which trigger these actions (`config/controllers.js`)
 *                 and/or override them with custom stationss (`config/stationss.js`)
 *
 *                 NOTE: The code you write here supports both HTTP and Socket.io automatically.
 *
 * @docs        :: http://sailsjs.org/#!documentation/controllers
 */
var googleapis = require('googleapis');
var jwt = new googleapis.auth.JWT(
		'424930963222-s59k4k5usekp20guokt0e605i06psh0d@developer.gserviceaccount.com', 
		'availwim.pem', 
		'3d161a58ac3237c1a1f24fbdf6323385213f6afc', 
		['https://www.googleapis.com/auth/bigquery']
	);
jwt.authorize();	
var bigQuery = googleapis.bigquery('v2');

module.exports = {    
  	
	stationsGeo:function(req,res){

		var stationsCollection = {};
		stationsCollection.type = "FeatureCollection";
		stationsCollection.features = [];

		var sql = 'SELECT station_id,state_code,ST_AsGeoJSON(the_geom) station_location FROM tmas where num_lane_1 > 0 group by station_id,state_code,ST_AsGeoJSON(the_geom);'
		Stations.query(sql,{},function(err,data){
			if (err) {res.send('{status:"error",message:"'+err+'"}',500);return console.log(err);}
				data.rows.forEach(function(stations){
					var stationsFeature = {};
					stationsFeature.type="Feature";
					stationsFeature.geometry = JSON.parse(stations.station_location);
					stationsFeature.properties = {};
					stationsFeature.properties.station_id = stations.station_id;
					stationsFeature.properties.state_code = stations.state_code;
					stationsCollection.features.push(stationsFeature);

					});

			res.send(stationsCollection);
		});
 	},
	ClassStationsGeo:function(req,res){

		var stationsCollection = {};
		stationsCollection.type = "FeatureCollection";
		stationsCollection.features = [];

		var sql = 'SELECT  station_id,state_code,ST_AsGeoJSON(the_geom) station_location FROM tmas where num_lanes1 > 0 group by station_id,state_code,ST_AsGeoJSON(the_geom);'
		Stations.query(sql,{},function(err,data){
			if (err) {res.send('{status:"error",message:"'+err+'"}',500);return console.log(err);}
				data.rows.forEach(function(stations){
					var stationsFeature = {};
					stationsFeature.type="Feature";
					stationsFeature.geometry = JSON.parse(stations.station_location);
					stationsFeature.properties = {};
					stationsFeature.properties.station_id = stations.station_id;
					stationsFeature.properties.state_code = stations.state_code;
					stationsCollection.features.push(stationsFeature);

					});

			res.send(stationsCollection);
		});
 	},
 	getAllWimStations:function(req,res){
 		var database = req.param('database');
 		console.time('total-AllWim');

	    var sql = 'SELECT state_fips,station_id,count(1) AS num_trucks '+
				    'FROM [tmasWIM12.'+database+'] '+
				    'WHERE state_fips IS NOT NULL '+
				    'GROUP BY state_fips,station_id '+
				    'ORDER BY state_fips,num_trucks DESC;';
		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },
	    function(err, response) {
      		if (err) console.log('Error:',err);
      		
      		res.json(response)

	    });
 	},
 	getAllClassStations: function(req,res){
 		var database = req.param('database')+'Class';

 		console.time('total-AllClass');

 		//console.time('auth-AllClass');
 		var sql = 'SELECT state_fips,station_id,sum(total_vol) AS num_trucks '+
					    'FROM [tmasWIM12.'+database+'] '+
					    'WHERE state_fips IS NOT NULL '+
					    'GROUP BY state_fips,station_id '+
					    'ORDER BY state_fips,num_trucks DESC;';

		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
      		if (err) console.log('Error:',err);
      		
      		res.json(response)
	    });
 	},

 	getStateWimStations: function(req,res) {
 		if(typeof req.param('stateFips') == 'undefined'){
 			res.send('{status:"error",message:"state FIPS required"}',500);
 			return;
 		}
 		console.time('total-wimState');
 		var state_fips = req.param('stateFips'),
 			database = req.param('database');

 		//console.time('auth-wimState');
 		var sql = 'select station_id, year,count( distinct num_months) as numMon, '+
		    		  'count(distinct num_days) as numDay, count(distinct num_hours)/8760 as percent, '+
		    		  'sum(total)/count(distinct num_days) as AADT '+
		    		  'from (select  station_id,year,concat(string(year),string(month)) as num_months, '+
		    				  'concat(string(year),string(month),string(day)) as num_days, '+
		    				  'concat(string(year),string(month),string(day),string(hour)) as num_hours, '+
		    				  'count(station_id) as total '+
		    				  'FROM [tmasWIM12.'+database+'] '+
		    				  'where state_fips="'+state_fips+'" '+
		    				  'and state_fips is not null '+
		    				  'group by station_id,year,num_hours,num_months,num_days) '+
		    			'group by station_id,year '+
		    			'order by station_id,year';
		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
      		if (err) console.log('Error:',err);
      		
      		res.json(response)
	    });
 	},
 	getStateClassStations: function(req,res) {
 		if(typeof req.param('statefips') == 'undefined'){
 			res.send('{status:"error",message:"state FIPS required"}',500);
 			return;
 		}
 		console.time('total-classState');
 		var state_fips = req.param('statefips'),
 			database = req.param('database')+'Class';

 		//console.time('auth-classState');
 		var sql = 'select station_id,year,avg(DT) as ADT ,avg(passenger) as APT,'+
		    'avg(SU) as ASU ,avg(TT) as ATT from (select station_id,year,month,day,sum(total_vol) as DT,'+
		    ' sum(class1+class2+class3) as passenger, sum(class4+class5+class6+class7) as SU,'+
		    ' sum(class8+class9+class10+class11+class12+class13) as TT from [tmasWIM12.'+database+'] where '+
		    ' state_fips ="'+state_fips+'" group by state_fips,station_id,year,month,day order by station_id, '+
		    'station_id,year,month,day) group by station_id,year'

		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
      		if (err) console.log('Error:',err);
      		
      		res.json(response)
	    });
 	},
 	getStationGeoForState: function(req, res) {
 		if(typeof req.param('statefips') == 'undefined'){
 			res.send('{status:"error",message:"state FIPS required"}',500);
 			return;
 		}
 		var stateFIPS = +req.param('statefips');
 		if(parseInt(stateFIPS)<10){
 			stateFIPS = "0"+stateFIPS
 		}
 		
			var featureCollection = {
				type: "FeatureCollection",
				features: []
			}

			var sql = "SELECT station_id,func_class_code,method_of_vehicle_class,"+
				"method_of_truck_weighing,type_of_sensor,latitude,longitude "+
				"FROM [tmasWIM12.allStations] "+
				"WHERE NOT latitude = '        ' AND NOT latitude = '       0'"+
				"AND state_fips = '" +stateFIPS+ "' "+
				"GROUP BY station_id,func_class_code,method_of_vehicle_class,"+
				"method_of_truck_weighing,type_of_sensor,latitude,longitude;";

		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
      		if (err) console.log('Error:',err);
      	

			    	var schema = [];
			    	response.schema.fields.forEach(function(d) {
			    		schema.push(d.name);
			    	})
			    	response.rows.forEach(function(d) {
			    		var feature = {
			    			type:'Feature',
			    			geometry: {
			    				type:'Point',
			    				coordinates: [0, 0]
			    			},
			    			properties: {}
			    		};
			    		schema.forEach(function(name, i) {
			    			if (name != 'latitude' && name != 'longitude') {
				    			feature.properties[name] = d.f[i].v;
				    		} else if (name == 'longitude') {
				    			var lng = (+d.f[i].v).toString();
				    			if (/^1/.test(lng)) {
				    				lng = lng.replace(/^(1\d\d)/, '-$1.');
				    			} else {
				    				lng = lng.replace(/^(\d\d)/, '-$1.');
				    			}

				    			feature.geometry.coordinates[0] = lng*1;
				    		} else if (name == 'latitude') {
				    			var lat = (+d.f[i].v).toString().replace(/^ ?(\d\d)/, '$1.');
				    			feature.geometry.coordinates[1] = lat*1;
				    		}
			    		})
			    		featureCollection.features.push(feature);
			    	})

	          		res.json(featureCollection);
	        	});
 	},
 	getClassStationData: function(req, res) {
 		if(typeof req.param('id') == 'undefined'){
 			res.send('{status:"error",message:"station_id required"}',500);
 			return;
 		}
 		var station_id = req.param('id'),
 			depth = req.param('depth'),
 			database = req.param('database')+'Class';

 		var select = {
 			1: 'year',
 			2: 'month',
 			3: 'day',
 			4: 'hour'
 		};

 		var SQL = generateSQL();

 		console.time('getClassStationData Query');
 		console.log("getclassstationData ",SQL)
 		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:SQL,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
      		if (err) console.log('Error:',err);
      		
      		console.timeEnd('getClassStationData Query');

      		res.json(response)
	    });

		function generateSQL() {
 			var sql	= "SELECT " + select[depth.length] + ", sum(total_vol) AS amount, "
 				+ addClasses()
 				+ "FROM [tmasWIM12."+database+"] "
 				+ "WHERE station_id = '"+station_id+"' "
 				+ addPredicates()
 				+ "GROUP BY " + select[depth.length] + " "
 				+ "ORDER BY " + select[depth.length] + ";";

			return sql;
		}
		function addClasses() {
			var classes = '';

			for (var i = 1; i < 14; i++) {
				classes += 'sum(class'+i+') AS class'+i+', '
			}
			return classes;
		}
 		function addPredicates() {
 			var preds = '';
 			for (var i = 1; i < depth.length; i++) {
 				preds += 'AND ' + select[i] + ' = ' + depth[i] + ' ';
 			}
 			return preds;
 		}
 	},
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
 		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '1',
	    	resource: {query:SQL,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
      		if (err) console.log('Error:',err);
      		console.timeEnd('getWimStationDataQuery')
      		console.time('getWimStationDataSend')
      		res.json(response)
      		console.timeEnd('getWimStationDataSend')
	    });
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
 	getDailyWeights:function(req,res){
 		//console.log('cal get trucks', req.param('database'));
 		console.log('getDailyWeightInfoQuery')
 		var station_id = req.param('id'),
 			database = req.param('database');
 		//console.time('auth');
 		var sql = 'SELECT num_days, count(num_days) AS numDay, '+
		    			   'month, day, class, year, '+
		    			   'sum(total_weight) '+
		    			   'FROM (SELECT station_id, class, '+
		    			   		 'concat(string(year), string(month), string(day)) AS num_days, '+
		    			   		 'month, day, year, total_weight '+
		    			   		 'FROM [tmasWIM12.'+database+'] '+
		    			   		 'WHERE station_id="'+station_id+'" '+
		    			   		 'AND station_id IS NOT null) '+
 						   'GROUP BY num_days, month, day, class, year';
 		console.time('getDailyWeightInfoQuery')
 		console.log("getDailyWeights ",sql)
		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
			console.log("dailyweight ",err)

      		if (err) console.log('Error:',err);
      		console.timeEnd('getDailyWeightInfoQuery')
      		res.json(response)
	    });
 	},
 	getYearsActive:function(req,res){
 		var station_id = req.param('id'),
 			database = req.param('database');

 		if (!station_id || !database) {
 			res.send('Error, must specify database and station name', 500);
 			return;
 		}
 		if(req.param('isClass')==="class"){
 			database = database+"Class"
 		}

 		var sql = 'SELECT min(year),max(year) FROM [tmasWIM12.'+database+'] WHERE station_id = "'+ station_id + '";';
 		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
      		if (err) console.log('Error:',err);
      		res.json(response)
	    });
 	},
 	getClassAmounts:function(req,res){
 		console.log('getClassAmountsInfoQuery')
 		var database = req.param('database'),
 		station_id = req.param('id');
 		var sql = 'SELECT year, month, day, '+
		    			   'sum(class1), sum(class2), sum(class3), '+
		    			   'sum(class4), sum(class5), sum(class6), '+
		    			   'sum(class7), sum(class8), sum(class9), '+
		    			   'sum(class10), sum(class11), '+
		    			   'sum(class12), sum(class13), '+

		    			   'FROM [tmasWIM12.'+database+'Class] '+
		    			   'WHERE station_id="'+station_id+'" '+
		    			   'GROUP BY year, month, day';
		console.time('getClassAmountsInfoQuery')
		console.log("getClassAmounts ",sql)
		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
			console.log("classamount error: ",err)
			if (err) console.log('Error:',err);
      		console.timeEnd('getClassAmountsInfoQuery')
      		res.json(response)
	    });
 	},
	getStationInfo:function(req,res){
 		var database = req.param('database'),
 			station_id = req.param('id');

 		var sql = 'SELECT func_class_code, num_lanes_direc_indicated, '+
			    			   'sample_type_for_traffic_vol, method_of_traffic_vol_counting, '+
			    			   'alg_for_vehicle_class, class_sys_for_vehicle_class, '+
			    			   'method_of_truck_weighing, calibration_of_weighing_sys, '+
			    			   'type_of_sensor, second_type_of_sensor, primary_purpose, '+
			    			   'year_station_established, fips_county_code, '+
			    			   'concurrent_route_signing, concurrent_signed_route_num, '+
			    			   'national_highway_sys, posted_sign_route_num, station_location, '+
			    			   'latitude, longitude,  '+
		    			   'FROM [tmasWIM12.allStations] '+
		    			   'WHERE station_id="'+station_id+'" '+
		    			   'LIMIT 1';

		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
      		if (err) console.log('Error:',err);
      		
      		res.json(response)
	    });
 	},
 	getStationTimeLine: function(req,res) {
 		if(typeof req.param('statefips') == 'undefined'){
 			res.send('{status:"error",message:"state FIPS required"}',500);
 			return;
 		}
 		if(typeof req.param('time') == 'undefined'){
 			res.send('{status:"error",message:"state FIPS required"}',500);
 			return;	
 		}
 		if(typeof req.param('year') == 'undefined'){
 			res.send('{status:"error",message:"state FIPS required"}',500);
 			return;	
 		}
 		var state_fips = req.param('statefips'),
 			time = req.param('time'),
 			year = req.param('year'),
 			database = req.param('database')+'Class';

 		if(time === 'hour'){
				if(year === 'All'){
					var sql = 'select station_id,hour,avg(DT) as ADT, avg(passenger) as APT,avg(SU) as AST,avg(TT) as ATT,classCode '+
		 			'from (select a.station_id,a.year,a.month,a.day,sum(a.total_vol) as DT,sum(a.class1+a.class2+a.class3) as passenger,'+
		 			' sum(a.class4+a.class5+a.class6+a.class7) as SU,sum(a.class8+a.class9+a.class10+a.class11+a.class12+a.class13) as TT, '+
		 			'b.func_class_code as classCode,a.hour from [tmasWIM12.'+database+'] as a join '+
		 			'(select station_id,func_class_code from [tmasWIM12.allStations] group by station_id, func_class_code) as b '+
		 			'on a.station_id = b.station_id where a.state_fips ="'+state_fips+'" group by a.state_fips,a.station_id,a.year,a.month,a.day,'+
		 			'classCode,a.hour) '+
					'group by station_id,hour,classCode'
				}
				else{
					var sql = 'select station_id,hour,avg(DT) as ADT, avg(passenger) as APT,avg(SU) as AST,avg(TT) as ATT,classCode '+
		 			'from (select a.station_id,a.year,a.month,a.day,sum(a.total_vol) as DT,sum(a.class1+a.class2+a.class3) as passenger,'+
		 			' sum(a.class4+a.class5+a.class6+a.class7) as SU,sum(a.class8+a.class9+a.class10+a.class11+a.class12+a.class13) as TT, '+
		 			'b.func_class_code as classCode,a.hour from [tmasWIM12.'+database+'] as a join '+
		 			'(select station_id,func_class_code from [tmasWIM12.allStations] group by station_id, func_class_code) as b '+
		 			'on a.station_id = b.station_id where a.state_fips ="'+state_fips+'" and a.year = '+year+'group by a.state_fips,a.station_id,a.year,a.month,a.day,'+
		 			'classCode,a.hour) '+
					'group by station_id,hour,classCode'
				}
			}
			else{
				if(year === 'All'){
					var sql = 'select station_id,month,avg(DT) as ADT, avg(passenger) as APT,avg(SU) as AST,avg(TT) as ATT,classCode '+
		 			'from (select a.station_id,a.year,a.month,a.day,sum(a.total_vol) as DT,sum(a.class1+a.class2+a.class3) as passenger,'+
		 			' sum(a.class4+a.class5+a.class6+a.class7) as SU,sum(a.class8+a.class9+a.class10+a.class11+a.class12+a.class13) as TT, '+
		 			'b.func_class_code as classCode from [tmasWIM12.'+database+'] as a join '+
		 			'(select station_id,func_class_code from [tmasWIM12.allStations] group by station_id, func_class_code) as b '+
		 			'on a.station_id = b.station_id where a.state_fips ="'+state_fips+'" group by a.state_fips,a.station_id,a.year,a.month,a.day,'+
		 			'classCode order by a.station_id, a.year,a.month,a.day) '+
					'group by station_id,month,classCode'
				}
				else{
					var sql = 'select station_id,month,avg(DT) as ADT, avg(passenger) as APT,avg(SU) as AST,avg(TT) as ATT,classCode '+
		 			'from (select a.station_id,a.year,a.month,a.day,sum(a.total_vol) as DT,sum(a.class1+a.class2+a.class3) as passenger,'+
		 			' sum(a.class4+a.class5+a.class6+a.class7) as SU,sum(a.class8+a.class9+a.class10+a.class11+a.class12+a.class13) as TT, '+
		 			'b.func_class_code as classCode from [tmasWIM12.'+database+'] as a join '+
		 			'(select station_id,func_class_code from [tmasWIM12.allStations] group by station_id, func_class_code) as b '+
		 			'on a.station_id = b.station_id where a.state_fips ="'+state_fips+'" and a.year = '+year+'group by a.state_fips,a.station_id,a.year,a.month,a.day,'+
		 			'classCode order by a.station_id, a.year,a.month,a.day) '+
					'group by station_id,month,classCode'	
				}
			}
		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
      		if (err) console.log('Error:',err);
      		
      		res.json(response)
	    });
 	},
 	getStateWeightStations: function(req,res) {
 		if(typeof req.param('stateFips') == 'undefined'){
 			res.send('{status:"error",message:"state FIPS required"}',500);
 			return;
 		}
 		var state_fips = req.param('stateFips'),
 			database = req.param('database');

 		var sql = 'select station_id,year,count(distinct num_hours) as hours,'+
		    		  'sum(weight) as weight,class from (select station_id,year, month,day,'+
		    		  'concat(string(year),string(month),string(day),string(hour)) as num_hours,'+
		    		  'sum(total_weight) as weight,class FROM [tmasWIM12.'+database+'] where state_fips="'+state_fips+'"'+
		    		  'group each by station_id,year,month,day,num_hours,class order by station_id,'+
		    		  'year,month,day,num_hours,class) group by station_id,year,class'
		
 		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
      		if (err) console.log('Error:',err);
      		
      		res.json(response)
	    });
 	},
 	getStateOverweightStations: function(req,res) {
 		if(typeof req.param('stateFips') == 'undefined'){
 			res.send('{status:"error",message:"state FIPS required"}',500);
 			return;
 		}
 		if(typeof req.param('timeType') == 'undefined'){
 			res.send('{status:"error",message:"time type required"}',500);
 			return;
 		}
 		if(typeof req.param('threshold') == 'undefined'){
 			res.send('{status:"error",message:"threshold required"}',500);
 			return;
 		}
 		var state_fips = req.param('stateFips'),
 			database = req.param('database'),
 			timeType = req.param('timeType'),
 			threshold = req.param('threshold');


 		if(timeType === "on"){
		    	var sql = 'select a.station_id, SUM(CASE WHEN a.total_weight*220.462 >= '+threshold+' THEN 1 ELSE 0 END) as overTrucks,count(1) as total_trucks,a.month,b.func_class_code from [tmasWIM12.'+database+'] as a join (select station_id,func_class_code from [tmasWIM12.allStations] group by station_id, func_class_code) as b on a.station_id = b.station_id where a.state_fips="'+state_fips+'"'+
		    	'and (a.class=8 or a.class=9 or a.class=10 or a.class=11 or a.class=12 or a.class=13) '+
		    	'group by a.station_id,a.month,a.year,b.func_class_code order by a.station_id,a.month,a.year,b.func_class_code'
		    }
		    else if(timeType === "year"){
			    var sql = 'select station_id, SUM(CASE WHEN total_weight*220.462 >= '+threshold+' THEN 1 ELSE 0 END) as overTrucks,count(1) as totalTrucks,year from [tmasWIM12.'+database+'] where state_fips="'+state_fips+'"'+
			    		  'and (class=8 or class=9 or class=10 or class=11 or class=12 or class=13) '+ 
			    		  'group by station_id,year order by station_id,year'
			   		}
	   		else if(timeType === "month"){
	   			var sql = 'select station_id, SUM(CASE WHEN total_weight*220.462 >= '+threshold+' THEN 1 ELSE 0 END) as overTrucks,count(1) as totalTrucks,month from [tmasWIM12.'+database+'] where state_fips="'+state_fips+'"'+
		    		  'and (class=8 or class=9 or class=10 or class=11 or class=12 or class=13) '+ 
		    		  'group by station_id,year,month order by station_id,year,month'
	   		}
	   		else if(timeType === "day"){
	   			var sql = 'select station_id, SUM(CASE WHEN total_weight*220.462 >= '+threshold+' THEN 1 ELSE 0 END) as overTrucks,count(1) as totalTrucks,day from [tmasWIM12.'+database+'] where state_fips="'+state_fips+'"'+
		    		  'and (class=8 or class=9 or class=10 or class=11 or class=12 or class=13) '+ 
		    		  'group by station_id,year,month,day order by station_id,year,month,day'
	   		}
	   	var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
      		if (err) console.log('Error:',err);
      		
      		res.json(response)
	    });
 	},
 	getWeightTableInfo: function(req,res) {
 		console.log('getWeightTableInfo')
 		if(typeof req.param('stationID') == 'undefined'){
 			res.send('{status:"error",message:"state FIPS required"}',500);
 			return;
 		}
 		var stationId = req.param('stationID'),
 			database = req.param('database');

 		var sql = 'select DAYOFWEEK(TIMESTAMP(concat(STRING(year),"-",STRING(month),"-",STRING(day)))) '+
		    'as week,hour,count(1),dir from [tmasWIM12.'+database+'] where station_id = "'+stationId+'" group by '+
		    'week,year,month,day,hour,dir order by week,year,month,day,hour,dir;'
		console.time('getWeightTableInfoQuery')
		console.log("gettableweight ",sql)
		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
			console.log("weight table error: ",err)
      		if (err) console.log('Error:',err);
      		console.timeEnd('getWeightTableInfoQuery')
      		res.json(response)
	    });
 	},
 	getTonageInfo: function(req,res) {
 		console.log('getTonageInfoQuery')
 		if(typeof req.param('stationID') == 'undefined'){
 			res.send('{status:"error",message:"state FIPS required"}',500);
 			return;
 		}
 		var stationId = req.param('stationID'),
 			database = req.param('database');
 		var empty_truck_tonage = 43558,
 			truck_class = 9
 		// var sql = 'SELECT SUM(case when ((total_weight*220.462)-'+empty_truck_tonage+') >= 0 and '+
 		// 	'((total_weight*220.462)-'+empty_truck_tonage+') < 10000 then total_weight else 0 end) as zero_tenVIUS,'+
 		// 	' SUM(case when ((total_weight*220.462)-'+empty_truck_tonage+') >= 10000 and ((total_weight*220.462)-'+empty_truck_tonage+')'+
 		// 	' < 20000 then total_weight else 0 end) as ten_twenVIUS, SUM(case when ((total_weight*220.462)-'+empty_truck_tonage+')'+
 		// 	' >= 20000 and ((total_weight*220.462)-'+empty_truck_tonage+') < 30000 then total_weight else 0 end) as twen_thirVIUS,'+
 		// 	' SUM(case when ((total_weight*220.462)-'+empty_truck_tonage+') >= 30000 and ((total_weight*220.462)-'+empty_truck_tonage+') < 40000 then'+
 		// 	' total_weight else 0 end) as thir_fortVIUS, SUM(case when ((total_weight*220.462)-'+empty_truck_tonage+') >= 40000 and'+
 		// 	' ((total_weight*220.462)-'+empty_truck_tonage+') < 50000 then total_weight else 0 end) as fort_fiftVIUS, SUM(case when'+
 		// 	' ((total_weight*220.462)-'+empty_truck_tonage+') >= 50000 then total_weight else 0 end) as great_fiftyVIUS,'+
 		// 	' SUM(case when total_weight*220.462 <= '+empty_truck_tonage+' then total_weight else 0 end) as empty_vius,'+
 		// 	' TIMESTAMP(concat(STRING(year),"-",STRING(month),"-",STRING(day))) as date,'+
 		// 	'DAYOFWEEK(TIMESTAMP(concat(STRING(year),"-",STRING(month),"-",STRING(day)))) as day_,'+
 		// 	' count(1) FROM [tmasWIM12.'+database+'] where class = '+truck_class+' and station_id = "'+stationId+'"" group by year,month,day,date,day_'
 		var sql = 'SELECT SUM(case when ((total_weight*220.462)-12209) >= 0 and class = 1 then total_weight else 0 end) ,'+
 			//' SUM(case when total_weight*220.462 <= '+empty_truck_tonage+' then total_weight else 0 end) as empty_vius,'+
 			' year,month,day, '+
 			'SUM(case when ((total_weight*220.462)-25189) >= 0 and class = 2 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-34303) >= 0 and class = 3 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-19539) >= 0 and class = 4 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-31464) >= 0 and class = 5 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-39739) >= 0 and class = 6 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-37000) >= 0 and class = 7 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-40000) >= 0 and class = 8 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-51935) >= 0 and class = 9 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-31000) >= 0 and class = 10 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-40000) >= 0 and class > 10 then total_weight else 0 end) ,'+
 			//'DAYOFWEEK(TIMESTAMP(concat(STRING(year),"-",STRING(month),"-",STRING(day)))) as day_, class = '+truck_class+'
 			/*' count(1)*/' FROM [tmasWIM12.'+database+'] where class = 9 and station_id = "'+stationId+'" group by year,month,day'
 		console.time('getTonageInfoQuery')
 		console.log("gettonnageinfo ",sql)
		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
      		if (err) console.log('Error:',err);
      		console.timeEnd('getTonageInfoQuery')
      		res.json(response)
	    });
 	},
 	getTonageStations: function(req,res) {
 		if(typeof req.param('stateFips') == 'undefined'){
 			res.send('{status:"error",message:"state FIPS required"}',500);
 			return;
 		}
 		var state_fips = req.param('stateFips'),
 			database = req.param('database');
 		var empty_truck_tonage = 43558,
 			truck_class = 9
 		/*In the future, may need to edit total weight formula*/
 		var sql = 'SELECT SUM(case when ((total_weight*220.462)-12209) >= 0 and class = 1 then total_weight else 0 end) ,'+
 			//' SUM(case when total_weight*220.462 <= '+empty_truck_tonage+' then total_weight else 0 end) as empty_vius,'+
 			'SUM(case when ((total_weight*220.462)-25189) >= 0 and class = 2 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-34303) >= 0 and class = 3 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-19539) >= 0 and class = 4 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-31464) >= 0 and class = 5 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-39739) >= 0 and class = 6 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-37000) >= 0 and class = 7 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-40000) >= 0 and class = 8 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-51935) >= 0 and class = 9 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-31000) >= 0 and class = 10 then total_weight else 0 end) ,'+
 			'SUM(case when ((total_weight*220.462)-40000) >= 0 and class > 10 then total_weight else 0 end) ,'+
 			'station_id'+
 			' year,month,day, '+
 			//'DAYOFWEEK(TIMESTAMP(concat(STRING(year),"-",STRING(month),"-",STRING(day)))) as day_, class = '+truck_class+'
 			/*' count(1)*/' FROM [tmasWIM12.'+database+'] where class = 9 and state_fips = "'+state_fips+'" group by station_id,year,month,day'
 			//console.log(sql)
 		var request = bigQuery.jobs.query({
	    	kind: "bigquery#queryRequest",
	    	projectId: 'avail-wim',
	    	timeoutMs: '10000',
	    	resource: {query:sql,projectId:'avail-wim'},
	    	auth: jwt
	    },

		function(err, response) {
			if (err) console.log('Error:',err);
 			res.json(response)
	    });
 	},
 	


  /**
   * Overrides for the settings in `config/controllers.js`
   * (specific to StationsController)
   */
  _config: {}

  
};
