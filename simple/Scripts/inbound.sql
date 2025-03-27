SELECT XBTFTY, XBPRTY, XBLBNM, XBDBNM, XBMBNM, XBIFSF, XBBCTN, XBTFMG, XBBGDT, XBBGTI, XBEDDT, XBEDTI, XBSTAT, XBSTMS, XBFTP, XBFLEN, XBTUID, XBRCEX, XBCREX, XBPDCR, XBPTCR, XBPDLM, XBPTLM, XBUSER, CDC_SRC_LAST_UPDATE_DATE, CDC_RPL_LAST_UPDATE_DATE
FROM WMBD1_PRODUCTION.WMS.XBCTRL00;

SELECT 'BDC' BU, XBTFTY, XBMBNM||'.TXT' File, XBPDLM, XBPTLM
FROM WMBD1_PRODUCTION.WMS.XBCTRL00
WHERE XBTFTY IN ('OG1')
AND XBSTAT ='90' ORDER BY XBPDLM ;

 
SELECT * FROM E1_PRODUCTION.E1.F550006A;

SELECT gcmcu, GC55TSET,gcrefn, GCEDER, gc55upmjo, GC55TDAYO, GCUPMJ, GCTDAY 
FROM E1_PRODUCTION.E1.F550006A
WHERE GC55LACT ='95' AND GCMCU IN ('BDC') AND gc55tset='WMSPIX';


---
---cast( substr( right( '00' || gc55tdayo, 6) ,1,2) || ':' || substr( right( '00' || gc55tdayo, 6) ,3,2) || ':' || substr( right( '00' || gc55tdayo, 6) ,5,2) as time)
--Union
SELECT 'NDC' BU, XBTFTY, XBMBNM||'.TXT' File, XBPDLM, XBPTLM , gc55upmjo, GC55TDAYO, GCUPMJ, GCTDAY,    -- GC55TDAYO
cast( substr( right( '00' || gc55tdayo, 6) ,1,2) || ':' || substr( right( '00' || gc55tdayo, 6) ,3,2) || ':' || substr( right( '00' || gc55tdayo, 6) ,5,2) as time),
TIMESTAMP_FROM_PARTS(CAST('2024-05-06' AS char), CAST('16:00:00' AS char))
--TIMESTAMP_FROM_PARTS(gc55upmjo,   
--cast( substr( right( '00' || gc55tdayo, 6) ,1,2) || ':' || substr( right( '00' || gc55tdayo, 6) ,3,2) || ':' || substr( right( '00' || gc55tdayo, 6) ,5,2) as time), 
--108)
FROM WMNDC_PRODUCTION.WMS.XBCTRL00 INNER JOIN E1_PRODUCTION.E1.F550006A  ON XBMBNM||'.TXT' = GCREFN 
WHERE XBTFTY IN ('OT2') AND XBSTAT ='90' 
AND GC55LACT ='95' AND GCMCU IN ('NDC') AND gc55tset='WMSSHC' and gcrefn ='FO445459.TXT';
----------------------------------------------------------------------------------------------------------------------------
ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = TIMESTAMP_NTZ;
WITH
lineserp AS (
SELECT gcmcu, GC55TSET,gcrefn, GCEDER, gc55upmjo Fdate, 
cast( substr( right( '0000' || gc55tdayo, 6) ,1,2) || ':' || substr( right( '0000' || gc55tdayo, 6) ,3,2) || ':' || substr( right( '0000' || gc55tdayo, 6) ,5,2) as time) AS Ftime, --GC55TDAYO, 
GCUPMJ Tdate,
cast( substr( right( '0000' || gctday, 6) ,1,2) || ':' || substr( right( '0000' || gctday, 6) ,3,2) || ':' || substr( right( '0000' || gctday, 6) ,5,2) as time) AS Ttime --GCTDAY 
FROM E1_PRODUCTION.E1.F550006A
WHERE GC55LACT ='95' AND GCMCU IN ('NDC') AND gc55tset='WMSSHC'
--AND gcrefn='FO426435.TXT'   --'FO389133.TXT'
),
lineserpts AS (
SELECT a.*, 
timestamp_from_parts(fdate, ftime) AS TSFrom,
timestamp_from_parts(tdate, ttime) AS TSto,
FROM lineserp a ) 
SELECT a.*, TIMESTAMPDIFF('second',TSFrom, TSto) FROM lineserpts a 
--SELECT a.* FROM lineserpts a 
;
----------------------------------------------------------------------------------------------------------------------------
---using        NDC NDC 
--TIME(CONCAT(SUBSTR(LPAD(field_name,6,0),1,2),':',SUBSTR(LPAD(field_name,6,0),3,2),':',SUBSTR(LPAD(field_name,6,0),5,2)))  
----------------------------------------------------------------------------------------------------------------------------

WITH
lineswms AS (
SELECT 'NDC' BU, XBMBNM||'.TXT' WFile, timestamp_from_parts(XBPDLM , xbptlm) AS WMFrom
FROM WMNDC_PRODUCTION.WMS.XBCTRL00 
WHERE XBTFTY IN ('OG2') AND XBSTAT ='90' 
ORDER BY xbmbnm 
),
lineserp AS (
SELECT gcmcu,gcrefn, gc55upmjo Fdate, 
TIME(CONCAT(SUBSTR(LPAD(gc55tdayo,6,0),1,2),':',SUBSTR(LPAD(gc55tdayo,6,0),3,2),':',SUBSTR(LPAD(gc55tdayo,6,0),5,2))) AS Ftime,
GCUPMJ Tdate,
TIME(CONCAT(SUBSTR(LPAD(gctday,6,0),1,2),':',SUBSTR(LPAD(gctday,6,0),3,2),':',SUBSTR(LPAD(gctday,6,0),5,2))) AS Ttime
FROM E1_PRODUCTION.E1.F550006A
WHERE GC55LACT ='95' AND GCMCU IN ('NDC') AND gc55tset='WMSSHC'
--AND gcrefn='FO426435.TXT'   --'FO389133.TXT'
)
,
onefile1 AS (
SELECT bu, fdate,WFILE, wmfrom,
timestamp_from_parts(fdate, ftime) AS TSFrom,
timestamp_from_parts(tdate, ttime) AS TSto
FROM lineswms INNER JOIN lineserp ON bu=gcmcu AND wfile = gcrefn  ---  
ORDER BY 1,2
),
onefile2 AS (
SELECT a.*, 
ROUND(TIMESTAMPDIFF('second',WMfrom, TSFrom)/60,2) AS DiffminWF,
ROUND(TIMESTAMPDIFF('second',TSFrom, TSto  )/60,2) AS DiffminFT,
ROUND(TIMESTAMPDIFF('second',WMFrom, TSto  )/60,2) AS DiffminTO
FROM onefile1 a
),
onefile3 AS (
SELECT a.*, CASE 
				WHEN DiffminWF BETWEEN  0.00 AND  5.00 THEN 1
				WHEN DiffminWF BETWEEN  5.01 AND 10.00 THEN 2
				WHEN DiffminWF BETWEEN 10.01 AND 15.00 THEN 3
				WHEN DiffminWF BETWEEN 15.01 AND 20.00 THEN 4
				WHEN DiffminWF BETWEEN 20.01 AND 25.00 THEN 5
				WHEN DiffminWF BETWEEN 25.01 AND 30.00 THEN 6
				ELSE 7
			END AS DiffBWM,
			CASE 
				WHEN DiffminFT BETWEEN  0.00 AND  5.00 THEN 1
				WHEN DiffminFT BETWEEN  5.01 AND 10.00 THEN 2
				WHEN DiffminFT BETWEEN 10.01 AND 15.00 THEN 3
				WHEN DiffminFT BETWEEN 15.01 AND 20.00 THEN 4
				WHEN DiffminFT BETWEEN 20.01 AND 25.00 THEN 5
				WHEN DiffminFT BETWEEN 25.01 AND 30.00 THEN 6
				ELSE 7
			END AS DiffBFT,
			CASE 
				WHEN DiffminTO BETWEEN  0.00 AND  5.00 THEN 1
				WHEN DiffminTO BETWEEN  5.01 AND 10.00 THEN 2
				WHEN DiffminTO BETWEEN 10.01 AND 15.00 THEN 3
				WHEN DiffminTO BETWEEN 15.01 AND 20.00 THEN 4
				WHEN DiffminTO BETWEEN 20.01 AND 25.00 THEN 5
				WHEN DiffminTO BETWEEN 25.01 AND 30.00 THEN 6
				ELSE 7
			END AS DiffBTO			
FROM onefile2 a
)
--SELECT fdate, diffbucked, count(*) FROM linesdifflevel a GROUP BY fdate,diffbucked ORDER BY 1,2
SELECT * FROM onefile3 
ORDER BY 1, 2, 3
;

--*****************************************************************************************************************************
--
-- SC1/SC2/SC3/SC4  FO
-- 
--*****************************************************************************************************************************
ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = TIMESTAMP_NTZ;
WITH
lineswms AS (
SELECT 'SC1' BU, XBMBNM||'.TXT' WFile, timestamp_from_parts(XBPDLM , xbptlm) AS WMFrom
FROM WMSC1_PRODUCTION.WMS.XBCTRL00 
WHERE XBTFTY IN ('OG1') AND XBSTAT ='90' 
ORDER BY xbmbnm 
),
lineserp AS (
SELECT gcmcu,gcrefn, MIN(gc55upmjo) Fdate, 
MIN(TIME(CONCAT(SUBSTR(LPAD(gc55tdayo,6,0),1,2),':',SUBSTR(LPAD(gc55tdayo,6,0),3,2),':',SUBSTR(LPAD(gc55tdayo,6,0),5,2)))) AS Ftime,
MAX(GCUPMJ) Tdate,
MAX(TIME(CONCAT(SUBSTR(LPAD(gctday,6,0),1,2),':',SUBSTR(LPAD(gctday,6,0),3,2),':',SUBSTR(LPAD(gctday,6,0),5,2)))) AS Ttime
FROM E1_PRODUCTION.E1.F550006A
WHERE GC55LACT ='95' AND GCMCU IN ('SC1','SC2','SC3','SC4') AND gc55tset='WMSPIX'     --gc55tset='WMSSHC'
GROUP BY GCMCU, GCREFN
ORDER BY 1,2
--AND gcrefn='FO426435.TXT'   --'FO389133.TXT'
)
,
onefile1 AS (
SELECT bu, fdate,WFILE, wmfrom,
timestamp_from_parts(fdate, ftime) AS TSFrom,
timestamp_from_parts(tdate, ttime) AS TSto
FROM lineswms INNER JOIN lineserp ON wfile = gcrefn  ---bu=gcmcu AND  
ORDER BY 1,2
),
onefile2 AS (
SELECT a.*, 
ROUND(TIMESTAMPDIFF('second',WMfrom, TSFrom)/60,2) AS DiffminWF,
ROUND(TIMESTAMPDIFF('second',TSFrom, TSto  )/60,2) AS DiffminFT,
ROUND(TIMESTAMPDIFF('second',WMFrom, TSto  )/60,2) AS DiffminTO
FROM onefile1 a
),
onefile3 AS (
SELECT a.*, CASE 
				WHEN DiffminWF BETWEEN  0.00 AND  5.00 THEN 1
				WHEN DiffminWF BETWEEN  5.01 AND 10.00 THEN 2
				WHEN DiffminWF BETWEEN 10.01 AND 15.00 THEN 3
				WHEN DiffminWF BETWEEN 15.01 AND 20.00 THEN 4
				WHEN DiffminWF BETWEEN 20.01 AND 25.00 THEN 5
				WHEN DiffminWF BETWEEN 25.01 AND 30.00 THEN 6
				ELSE 7
			END AS DiffBWM,
			CASE 
				WHEN DiffminFT BETWEEN  0.00 AND  5.00 THEN 1
				WHEN DiffminFT BETWEEN  5.01 AND 10.00 THEN 2
				WHEN DiffminFT BETWEEN 10.01 AND 15.00 THEN 3
				WHEN DiffminFT BETWEEN 15.01 AND 20.00 THEN 4
				WHEN DiffminFT BETWEEN 20.01 AND 25.00 THEN 5
				WHEN DiffminFT BETWEEN 25.01 AND 30.00 THEN 6
				ELSE 7
			END AS DiffBFT,
			CASE 
				WHEN DiffminTO BETWEEN  0.00 AND  5.00 THEN 1
				WHEN DiffminTO BETWEEN  5.01 AND 10.00 THEN 2
				WHEN DiffminTO BETWEEN 10.01 AND 15.00 THEN 3
				WHEN DiffminTO BETWEEN 15.01 AND 20.00 THEN 4
				WHEN DiffminTO BETWEEN 20.01 AND 25.00 THEN 5
				WHEN DiffminTO BETWEEN 25.01 AND 30.00 THEN 6
				ELSE 7
			END AS DiffBTO			
FROM onefile2 a
)
--SELECT fdate, diffbucked, count(*) FROM linesdifflevel a GROUP BY fdate,diffbucked ORDER BY 1,2
SELECT * FROM onefile3 
ORDER BY 1, 2, 3
;


