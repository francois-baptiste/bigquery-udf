CREATE TEMP FUNCTION
  ST_SIMPLIFY3D(myarray ARRAY<STRUCT<x FLOAT64, y FLOAT64,z FLOAT64>> , tolerance NUMERIC)
  RETURNS ARRAY<STRUCT<x FLOAT64, y FLOAT64, z FLOAT64>>
  LANGUAGE js AS """
var highQuality = true;

return simplify(myarray, tolerance, highQuality);

  """ OPTIONS ( library=["gs://bigquery-geolib/simplify.js"] );

With T as (SELECT[
    STRUCT(0.,0.,0.),
    STRUCT(0.,0.,1.),
    STRUCT(0.,1.,1.),
    STRUCT(1.,1.,1.),
    STRUCT(1.,1.,3.),
    STRUCT(1.,3.,3.),
    STRUCT(3.,3.,3.),
    STRUCT(5.,5.,5.)]
     as myarray
     union all
  SELECT[
    STRUCT(0.,0.,0.),
    STRUCT(3.,3.,3.),
    STRUCT(0.,0.,1.),
    STRUCT(1.,1.,1.),
    STRUCT(1.,1.,3.),
    STRUCT(0.,1.,1.),
    STRUCT(1.,3.,3.),
    STRUCT(5.,5.,5.)]
    )
SELECT ST_SIMPLIFY3D(myarray, 1.)
From T

