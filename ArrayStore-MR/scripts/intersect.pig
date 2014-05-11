/* 
-- Local Load
csp = load 'cloudsat_test.gz' using PigStorage(',') as (lat:double, lon:double, y:int, d:int, t:long, a1:double, a2:double, a3:double, a4:double, a5:double, a6:double, a7:double);
cs = filter csp by lat < 38 and lat > -38; 
tr = load 'trmm_test.gz' using PigStorage(',') as (lat:double, lon:double, y:int, d:int, t:long, a1:double, a2:double, a3:double, a4:double, a5:double, a6:double, a7:double, a8:double, a9:double, a10:double);
*/

/*
Summary: 
	cs: cloudsat satellite (up-down)
	tr: trmm satellite (left-right)
*/

-- LOADING DATA
-- y: year, d: day of year, t: miliseconds offset from the start of the day
-- pi/180 = 0.01745329, R = 6353000
csp = load 'cloudsat/text/cloudsat2007' using PigStorage(',') as (lat:double, lon:double, y:int, d:int, t:long, a1:double, a2:double, a3:double, a4:double, a5:double, a6:double, a7:double);
csp = foreach csp generate -6353000 * COS(lat * 0.01745329) * COS(lon * 0.01745329) / SQRT( as x, 6353000 * COS(lat * 0.01745329)) as y, 6353000 * COS(lat * 0.01745329) * COS(lon * 0.01745329),  FLATTEN(csp); 
tr = load 'trmm/text/trmm2007' using PigStorage(',') as (lat:double, lon:double, y:int, d:int, t:long, a1:double, a2:double, a3:double, a4:double, a5:double, a6:double, a7:double, a8:double, a9:double, a10:double);

-- csp = load 'cloudsat_test/part*' using PigStorage('\t') as (lat:double, lon:double, y:int, d:int, t:long, a1:double, a2:double, a3:double, a4:double, a5:double, a6:double, a7:double);
-- tr = load 'trmm_test/part*' using PigStorage('\t') as (lat:double, lon:double, y:int, d:int, t:long, a1:double, a2:double, a3:double, a4:double, a5:double, a6:double, a7:double, a8:double);

-- csp = filter csp by d == 111;
-- tr = filter tr by d == 111;

/* OPT - Filtering any records that are not within possible intersect regions */
-- TRMM satellite are within latitude -38 to 38 degree
cs = filter csp by lat < 38 and lat > -38; 
cs = foreach cs generate lat, lon, y, d, t, t + 240000 * lon as tlocal, a1, a2, a3, a4, a5, a6, a7;
tr = foreach trp generate lat, lon, y, d, t, t + 240000 * lon as tlocal, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10;
tr = filter  

/* GRID - Grid by latitude, longitude, and time (15 minutes).
	At the moment, ignore the problem of satellite at borders of latitude or longitude (e.g. longitude after 180 is -180);
*/
cs_grid = foreach cs generate lat, lon, y, d, t, (int)(lat*10) as glat, (int)(lon*10) as glon, (int)(t/900000) as gts;
tr_grid = foreach tr generate lat, lon, y, d, t, (int)(lat*10) as glat, (int)(lon*10) as glon, (int)(t/900000) as gts;

/* JOIN - Equi join using gridded data, then select the best match for intersections */
-- Join
cs_tr = join cs_grid by (y, d, glat, glon, gts), tr_grid by (y, d, glat, glon, gts);
-- Add time difference, as well as which half (face) of the earth the satellite is at (since 2 satellite can only intersect once on each side in each of their orbits).
-- TODO: ignore distance computation for not
/* Compute distance
	dlon = lon2 - lon1 
	dlat = lat2 - lat1 
	a = (sin(dlat/2))^2 + cos(lat1) * cos(lat2) * (sin(dlon/2))^2 
	c = 2 * atan2( sqrt(a), sqrt(1-a) ) 
	d = R * c (where R is the radius of the Earth) =  6371 * 2 * atan
*/
cs_tr1 = foreach cs_tr generate cs_grid::y as y, cs_grid::d as d, cs_grid::glat/ABS(cs_grid::glat) as face, ABS(cs_grid::t - tr_grid::t) as tdiff, cs_grid::lat as cs_lat, cs_grid::lon as cs_lon, cs_grid::t as cs_t, tr_grid::lat as tr_lat, tr_grid::lon as tr_lon, tr_grid::t as tr_t, cs_grid::glat as glat, cs_grid::glon as glon, cs_grid::gts as gts;
-- Get the record that is of smallest time difference as the intersection point
-- TODO: should change this to distance, rather than time.
cs_tr2 = group cs_tr1 by (y, d, gts);
cs_tr3 = foreach cs_tr2 { 
	elms = order cs_tr1 by tdiff ASC;
	top = limit elms 1;
	generate FLATTEN(group), flatten(top); 
}; 
dump cs_tr3;

store cs_tr3 into 'cloudsat_trmm2.txt';


/* Convert to XYZ Coordinates */
cs_xyz = foreach cs generate -COS(lat * 0.01745329) * COS(lon * 0.01745329) as x1, COS(lat * 0.01745329) as y1, COS(lat * 0.01745329) * COS(lon * 0.01745329) as z1, lat, lon, y, d, t, a1, a2, a3, a4, a5, a6, a7;
cs_grid = foreach cs_xyz generate ROUND(6353000*x1/SQRT(x1 * x1 + y1*y1 + z1*z1)) as xx, ROUND(6353000*y1/SQRT(x1 * x1 + y1*y1 + z1*z1)) as yy, ROUND(6353000*z1/SQRT(x1 * x1 + y1*y1 + z1*z1)) as zz, lat, lon, y, d, t, ROUND(t/900000) as gts , a1, a2, a3, a4, a5, a6, a7;

tr_xyz = foreach tr generate -COS(lat * 0.01745329) * COS(lon * 0.01745329) as x1, COS(lat * 0.01745329) as y1, COS(lat * 0.01745329) * COS(lon * 0.01745329) as z1, lat, lon, y, d, t, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10;
tr_grid = foreach tr_xyz generate ROUND(6353000*x1/SQRT(x1 * x1 + y1*y1 + z1*z1)) as xx, ROUND(6353000*y1/SQRT(x1 * x1 + y1*y1 + z1*z1)) as yy, ROUND(6353000*z1/SQRT(x1 * x1 + y1*y1 + z1*z1)) as zz, lat, lon, y, d, t, ROUND(t/900000) as gts, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10;

cs_tr = join cs_grid by (y, d, xx, yy, zz, gts), tr_grid by (y, d, xx, yy, zz, gts);



/* For Gridded Data only */
cs_grid = load 'intersect/scidb/2007/cloudsat_grid_xyz.gz' using PigStorage(',') as (gx:int, gy:int, gz:int, gts:int,  x:int, y:int, z:int, year:int, doy:int, t:int, a1:double, a2:double, a3:double, a4:double, a5:double, a6:double, a7:double);
tr_grid = load 'intersect/scidb/2007/trmm_grid_xyz.gz' using PigStorage(',') as (gx:int, gy:int, gz:int, gts:int,  x:int, y:int, z:int, year:int, doy:int, t:int, a1:double, a2:double, a3:double, a4:double, a5:double, a6:double, a7:double, a8:double, a9:double, a10:double);

cs_grid = load 'cloudsat_grid_xyz.gz' using PigStorage(',') as (gx:int, gy:int, gz:int, gts:int,  x:int, y:int, z:int, year:int, doy:int, t:int, a1:double, a2:double, a3:double, a4:double, a5:double, a6:double, a7:double);
tr_grid = load 'trmm_grid_xyz.gz' using PigStorage(',') as (gx:int, gy:int, gz:int, gts:int,  x:int, y:int, z:int, year:int, doy:int, t:int, a1:double, a2:double, a3:double, a4:double, a5:double, a6:double, a7:double, a8:double, a9:double, a10:double);


cs_tr = JOIN cs_grid BY (year, doy, gx, gy, gz, gts), tr_grid BY (year, doy, gx, gy, gz, gts); 
--cs_tr = join cs_grid by (cs_grid.year, cs_grid.doy, cs_grid.gx, cs_grid.gy, cs_grid.gz, cs_grid.gt), tr_grid by (tr_grid.year, tr_grid.doy, tr_grid.gx, tr_grid.gy, tr_grid.gz, tr_grid.gt);
cs_tr = foreach cs_tr generate cs_grid::year as year, cs_grid::doy as doy, cs_grid::gts as gts, ((cs_grid::x - tr_grid::x)*(cs_grid::x - tr_grid::x) + (cs_grid::y - tr_grid::y)*(cs_grid::y - tr_grid::y) + (cs_grid::z - tr_grid::z)*(cs_grid::z - tr_grid::z)) as distance, TOTUPLE(TOTUPLE(cs_grid::a1, cs_grid::a2, cs_grid::a3, cs_grid::a4, cs_grid::a5, cs_grid::a6, cs_grid::a7), TOTUPLE(tr_grid::a1, tr_grid::a2, tr_grid::a3, tr_grid::a4, tr_grid::a5, tr_grid::a6, tr_grid::a7, tr_grid::a8, tr_grid::a9, tr_grid::a10)) as attributes; 

cs_tr2 = group cs_tr by (year, doy, gts);
cs_tr3 = foreach cs_tr2 { 
	elms = order cs_tr by distance ASC;
	top = limit elms 1;
	generate FLATTEN(group), flatten(top); 
}; 

store cs_tr3 into 'intersect/pig/2007/cloudsat_trmm_grid_xyz.gz' using PigStorage();
store cs_tr3 into 'cloudsat_trmm_grid_xyz.gz' using PigStorage();