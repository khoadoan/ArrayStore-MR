csp = load 'output/cloudsat111_text.gz' using PigStorage(',') as (lat:double, lon:double, y:int, d:int, t:long, orbit:int, a1:double, a2:double, a3:double, a4:double, a5:double, a6:double, a7:double);
trp = load 'output/trmm111_text.gz' using PigStorage(',') as (lat:double, lon:double, y:int, d:int, t:long, orbit:int, a1:double, a2:double, a3:double, a4:double, a5:double, a6:double, a7:double, a8:double, a9:double, a10:double);

-- cs = foreach csp generate ROUND(lat * 10000), ROUND(lon * 1000), ROUND(y), ROUND(d), ROUND(t), a1, a2, a3, a4, a5, a6, a7;
-- store cs into 'cloudsat/scidb/cloudsat2007' using PigStorage(',');

-- trscidb = foreach trp generate ROUND(lat * 10000), ROUND(lon * 1000), ROUND(y), ROUND(d), ROUND(t), a1, a2, a3, a4, a5, a6, a7, a8, a9, a10;
-- store trscidb into 'trmm/scidb/trmm2007' using PigStorage(',');

g = foreach csp generate ROUND(6371*COS(lat)*SIN(lon)) as xx, ROUND(6371*SIN(lat)) as yy, ROUND(6371*COS(lat)*COS(lon)) as zz, ROUND(y) as y, ROUND(d) as d, ROUND(t/100) as t, orbit, a1, a2, a3, a4, a5, a6, a7, (ROUND(6371*SIN(lat)) > 0 ? 1:0) as f;
-- gg = foreach g generate ROUND(xx/10), ROUND(yy/10), ROUND(zz/10), ROUND(t/(15*60*1000)), xx, yy, zz, y, d, t, orbit, a1, a2, a3, a4, a5, a6, a7;
store g into 'output/cloudsat111_xyz_scidb.gz' using PigStorage(',');

g = foreach trp generate ROUND(6371*COS(lat)*SIN(lon)) as xx, ROUND(6371*SIN(lat)) as yy, ROUND(6371*COS(lat)*COS(lon)) as zz, ROUND(y) as y, ROUND(d) as d, ROUND(t/100) as t, orbit, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, (ROUND(6371*SIN(lat)) > 0 ? 1:0) as f;
--gg = foreach g generate ROUND(xx/10), ROUND(yy/10), ROUND(zz/10), ROUND(t/(15*60*1000)), xx, yy, zz, y, d, t, orbit, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10;
store g into 'output/trmm111_xyz_scidb.gz' using PigStorage(',');

/* 
	x = R cos(lat) sin(lon)
	y = R sin(lat)
	z = R cos(lat) cos(lon)
*/