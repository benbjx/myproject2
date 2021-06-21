CREATE OR REPLACE TABLE `DSG.fact_order_article_tmp{{ params.pattern_full }}` AS
  
WITH 
---**---
new_target_data as (
  ---- WEBSHOP
SELECT
  q.combiorderid,
  q.origin_id,
  q.combiordernumber,
  q.ordernumber,
  q.fulfillmenttype,
  q.fulfillingstoreid,
  q.customerhomestorenumber,
  q.customernumber,
  q.cardholderid,
  q.deliveryDate,
  q.deliveryType,
  q.pickupStoreid,
  q.validated_order_employee,
  COALESCE(q.creationtime,
    q.dana_ingestion_timestamp_min) creationtime,
  q.articleId,
  MAX(q.positionNumber) positionNumber,
  q.merchantname,
  SAFE_CAST(NULL AS STRING) AS orderlineid,
  q.replaceditemnumber,
  paymentType_OM,
  orderComments,
  round(sum(q.order_quantity),2) order_quantity,
  round(sum(CASE WHEN dana_ingestion_timestamp_min >= '2019-05-14 09:00:00' THEN sumNet
	 WHEN (IFNULL(correct.sum_order_article_net_price,0) = 0 OR articleId = 'DUMMY-FRAIS_LIV') THEN sumNet
	 ELSE ROUND(sumNet + (sumNetInclTaxesAndFees - correct.sum_order_article_net_price) * (sumNet / correct.sum_order_article_net_price),2) END),2) sumNet 
FROM (
  SELECT
    DISTINCT t.combiorderid,
	t.orderContext.origin.id AS origin_id,
    t.combiordernumber,
    t.ordernumber,
    t.fulfillmenttype,
    t.fulfillingstoreid,
    t.customerhomestorenumber,
    t.customernumber,
    t.cardholdernumber AS cardholderid,
    t.deliveryinformation.deliveryDate,
    t.deliveryinformation.deliveryType,
	SAFE_CAST(t.deliveryinformation.pickupStoreid AS INT64) AS pickupStoreid,
    t.ordercontext.employeeId AS validated_order_employee,
    t.pricesummary.sumNetInclTaxesAndFees,
    TIMESTAMP(FORMAT_TIMESTAMP('%F %T',lst.dana_ingestion_timestamp_first, 'Europe/Paris')) AS dana_ingestion_timestamp_min,
    item.articleId,
    item.quantity as order_quantity,
	CASE WHEN dana_ingestion_timestamp >= '2019-05-14 09:00:00' THEN item.priceData.sumnetdouble
	   ELSE item.priceData.sumNet END as sumNet,
    SAFE_CAST(item.positionNumber AS INT64) positionNumber,
    NULL AS replaceditemnumber,
    '' AS merchantname,
    TIMESTAMP(FORMAT_TIMESTAMP('%F %T',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',t.orderContext.creationTime)),'Europe/Paris')) AS creationtime,
	paymentData.paymentType as paymentType_OM,
	orderComments,
    RANK() OVER (PARTITION BY t.combiordernumber, t.ordernumber, item.articleid ORDER BY CASE t.orderstatus WHEN 'PENDING' THEN 1 WHEN 'CONFIRMED' THEN 2 WHEN 'PICKING' THEN 3 WHEN 'PICKED' THEN 4 WHEN 'IN_SHIPMENT' THEN 5 WHEN 'READY_FOR_PICKUP' THEN 6 WHEN 'DELIVERED' THEN 7 WHEN 'CANCELED' THEN 8 WHEN 'CANCELLED' THEN 9 ELSE 0 END DESC, CASE t.invoicestatus WHEN 'EMPTY' THEN 1 WHEN 'PENDING' THEN 2 WHEN 'SENT' THEN 11 WHEN 'CREATED' THEN 21 WHEN 'VOIDED' THEN 22 ELSE 0 END DESC, pricesummary.sumNetInclTaxesAndFees DESC) rnk
  FROM
    `metro-bi-dl-fra-prod.ingest_om.webshop` t
  JOIN (
    SELECT
      i.combiordernumber combiordernumber_last,
      i.ordernumber ordernumber_last,
      MIN(i.dana_ingestion_timestamp) dana_ingestion_timestamp_first,
      MAX(i.dana_ingestion_timestamp) dana_ingestion_timestamp_max
    FROM
      `metro-bi-dl-fra-prod.ingest_om.webshop` i
    LEFT JOIN
      UNNEST(items) AS item
    WHERE
      i.ordernumber IS NOT NULL
      AND PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris'),FORMAT_TIMESTAMP('%F', dana_ingestion_timestamp, 'Europe/Paris'))) >= '2018-06-20'
      AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
      AND PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris'),FORMAT_TIMESTAMP('%F', dana_ingestion_timestamp, 'Europe/Paris'))) {{ params.pattern_date }}
	  
    GROUP BY
      i.combiordernumber,
      i.ordernumber) lst
  ON
    lst.combiordernumber_last = t.combiordernumber
    AND lst.ordernumber_last = t.ordernumber
    AND lst.dana_ingestion_timestamp_first = t.dana_ingestion_timestamp
  LEFT JOIN
    UNNEST(items) AS item
  WHERE
    PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris'),FORMAT_TIMESTAMP('%F', dana_ingestion_timestamp, 'Europe/Paris'))) >= '2018-06-20'
    AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
    AND PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris'),FORMAT_TIMESTAMP('%F', dana_ingestion_timestamp, 'Europe/Paris'))) {{ params.pattern_date }}
		
) q
 LEFT JOIN `DSG.view_correct_article_price` correct
 on correct.ordernumber = q.ordernumber
WHERE
  q.rnk = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,17,18,19,20,21
UNION ALL
  ---- DROPSHIP
SELECT
  combiorderid,
  origin_id,
  combiordernumber,
  ordernumber,
  fulfillmenttype,
  fulfillingstoreid,
  customerhomestorenumber,
  customernumber,
  cardholderid,
  deliveryDate,
  deliveryType,
  pickupStoreid,
  validated_order_employee,
  COALESCE(creationtime,
    dana_ingestion_timestamp_min) creationtime,
  articleId,
  MAX(positionNumber) positionNumber,
  merchantname,
  SAFE_CAST(NULL as STRING) AS orderlineid,
  replaceditemnumber,
  paymentType_OM,
  orderComments,
  round(sum(order_quantity),2) order_quantity,
  round(sum(sumNet),2) sumnet
FROM (
  SELECT
    DISTINCT t.combiorderid,
	t.orderContext.origin.id AS origin_id,
    t.combiordernumber,
    t.ordernumber,
    t.fulfillmenttype,
    t.fulfillingstoreid,
    t.customerhomestorenumber,
    t.customernumber,
    t.cardholdernumber AS cardholderid,
    t.deliveryinformation.deliveryDate,
    t.deliveryinformation.deliveryType,
	SAFE_CAST(t.deliveryinformation.pickupStoreid AS INT64) AS pickupStoreid,
    t.ordercontext.employeeId AS validated_order_employee,
    TIMESTAMP(FORMAT_TIMESTAMP('%F %T',lst.dana_ingestion_timestamp_first, 'Europe/Paris')) AS dana_ingestion_timestamp_min,
    item.articleId,
    item.quantity as order_quantity,
    item.priceData.sumNet as sumNet,
    SAFE_CAST(item.positionNumber as INT64) positionNumber,
    NULL AS replaceditemnumber,
    '' AS merchantname,
    TIMESTAMP(FORMAT_TIMESTAMP('%F %T',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',t.orderContext.creationTime)),'Europe/Paris')) AS creationtime,
	paymentData.paymentType as paymentType_OM,
	orderComments,
    RANK() OVER (PARTITION BY t.combiordernumber, t.ordernumber, item.articleid ORDER BY CASE t.orderstatus WHEN 'PENDING' THEN 1 WHEN 'CONFIRMED' THEN 2 WHEN 'PICKING' THEN 3 WHEN 'PICKED' THEN 4 WHEN 'IN_SHIPMENT' THEN 5 WHEN 'READY_FOR_PICKUP' THEN 6 WHEN 'DELIVERED' THEN 7 WHEN 'CANCELED' THEN 8 WHEN 'CANCELLED' THEN 9 ELSE 0 END DESC, CASE t.invoicestatus WHEN 'EMPTY' THEN 1 WHEN 'PENDING' THEN 2 WHEN 'SENT' THEN 11 WHEN 'CREATED' THEN 21 WHEN 'VOIDED' THEN 22 ELSE 0 END DESC) rnk
  FROM
    `metro-bi-dl-fra-prod.ingest_om.dropship` t
  JOIN (
    SELECT
      i.combiordernumber combiordernumber_last,
      i.ordernumber ordernumber_last,
      MIN(i.dana_ingestion_timestamp) dana_ingestion_timestamp_first,
      MAX(i.dana_ingestion_timestamp) dana_ingestion_timestamp_max
    FROM
      `metro-bi-dl-fra-prod.ingest_om.dropship` i
    LEFT JOIN
      UNNEST(items) AS item
    WHERE
      i.ordernumber IS NOT NULL
      AND PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris'),FORMAT_TIMESTAMP('%F', dana_ingestion_timestamp, 'Europe/Paris'))) >= '2018-06-20'
      AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
      AND PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris'),FORMAT_TIMESTAMP('%F', dana_ingestion_timestamp, 'Europe/Paris'))) {{ params.pattern_date }}
	  
    GROUP BY
      i.combiordernumber,
      i.ordernumber) lst
  ON
    lst.combiordernumber_last = t.combiordernumber
    AND lst.ordernumber_last = t.ordernumber
    AND lst.dana_ingestion_timestamp_first = t.dana_ingestion_timestamp
  LEFT JOIN
    UNNEST(items) AS item
  WHERE
    PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris'),FORMAT_TIMESTAMP('%F', dana_ingestion_timestamp, 'Europe/Paris'))) >= '2018-06-20'
    AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
    AND PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris'),FORMAT_TIMESTAMP('%F', dana_ingestion_timestamp, 'Europe/Paris'))) {{ params.pattern_date }} 
	
) q
WHERE
  q.rnk = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,17,18,19,20,21
UNION ALL
  ---- MARKETPLACE
SELECT
  combiorderid,
  origin_id,
  combiordernumber,
  ordernumber,
  fulfillmenttype,
  fulfillingstoreid,
  customerhomestorenumber,
  customernumber,
  cardholderid,
  deliveryDate,
  deliveryType,
  pickupStoreid,
  validated_order_employee,
  COALESCE(creationtime,
    dana_ingestion_timestamp_min) creationTime,
  articleId,
  MAX(positionNumber) positionNumber,
  merchantname,
  orderlineid,
  replaceditemnumber,
  paymentType_OM,
  orderComments,
  round(sum(order_quantity),2) order_quantity,
  round(sum(sumNet),2) sumnet
FROM (
  SELECT
    DISTINCT t.combiorderid,
	t.orderContext.id AS origin_id,
    t.combiordernumber,
    t.ordernumber,
    t.fulfillmenttype,
    t.fulfillingstoreid,
    t.customerhomestorenumber,
    t.customernumber,
    t.cardholdernumber AS cardholderid,
    t.delivery.Date AS deliverydate,
    t.delivery.Type AS deliverytype,
	SAFE_CAST(t.pickup.storeid AS INT64) AS pickupStoreid,
    t.ordercontext.employeeId AS validated_order_employee,
    TIMESTAMP(FORMAT_TIMESTAMP('%F %T',lst.dana_ingestion_timestamp_first, 'Europe/Paris')) AS dana_ingestion_timestamp_min,
    item.articleId,
    item.quantity as order_quantity,
    item.sumNet as sumNet,
    item.orderlineid,
    SAFE_CAST(item.positionNumber AS INT64) positionNumber,
    NULL AS replaceditemnumber,
    merchantname,
    TIMESTAMP(FORMAT_TIMESTAMP('%F %T',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris')) AS creationtime,
	payment.Type as paymentType_OM,
	orderComments,
    RANK() OVER (PARTITION BY t.combiordernumber, t.ordernumber, item.articleid ORDER BY CASE t.orderstatus WHEN 'PENDING' THEN 1 WHEN 'CONFIRMED' THEN 2 WHEN 'PICKING' THEN 3 WHEN 'PICKED' THEN 4 WHEN 'IN_SHIPMENT' THEN 5 WHEN 'READY_FOR_PICKUP' THEN 6 WHEN 'DELIVERED' THEN 7 WHEN 'CANCELED' THEN 8 WHEN 'CANCELLED' THEN 9 ELSE 0 END DESC, CASE item.miraklOrderLineStatus WHEN 'EMPTY' THEN 1 WHEN 'PENDING' THEN 2 WHEN 'SENT' THEN 11 WHEN 'CREATED' THEN 21 WHEN 'VOIDED' THEN 22 ELSE 0 END DESC,pickup.StoreName DESC) rnk
  FROM
    `metro-bi-dl-fra-prod.ingest_om.marketplace` t
  JOIN (
    SELECT
      i.combiordernumber combiordernumber_last,
      i.ordernumber ordernumber_last,
      MIN(i.dana_ingestion_timestamp) dana_ingestion_timestamp_first,
      MAX(i.dana_ingestion_timestamp) dana_ingestion_timestamp_max
    FROM
      `metro-bi-dl-fra-prod.ingest_om.marketplace` i
    LEFT JOIN
      UNNEST(items) AS item
    WHERE
      i.ordernumber IS NOT NULL
      AND PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris'),FORMAT_TIMESTAMP('%F', dana_ingestion_timestamp, 'Europe/Paris'))) >= '2018-06-20'
      AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
      AND PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris'),FORMAT_TIMESTAMP('%F', dana_ingestion_timestamp, 'Europe/Paris'))) {{ params.pattern_date }}
	  
    GROUP BY
      i.combiordernumber,
      i.ordernumber) lst
  ON
    lst.combiordernumber_last = t.combiordernumber
    AND lst.ordernumber_last = t.ordernumber
    AND lst.dana_ingestion_timestamp_first = t.dana_ingestion_timestamp
  LEFT JOIN
    UNNEST(items) AS item
  WHERE
    PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris'),FORMAT_TIMESTAMP('%F', dana_ingestion_timestamp, 'Europe/Paris'))) >= '2018-06-20'
    AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
    AND PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris'),FORMAT_TIMESTAMP('%F', dana_ingestion_timestamp, 'Europe/Paris'))) {{ params.pattern_date }} 
	
) q
WHERE
  q.rnk = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,17,18,19,20,21
UNION ALL
  ---- FSD
SELECT DISTINCT
res.combiOrderId,
res.origin_id,
res.combiordernumber,
res.ordernumber,
res.fulfillmenttype,
res.fulfillingstoreid,
res.customerHomeStoreNumber,
res.customerNumber,
res.cardholderid,
res.deliverydate,
res.deliverytype,
NULL AS pickupStoreid,
res.validated_order_employee,
res.creationtime,
res.articleId,
res.positionNumber,
'' AS merchantname,
'' AS orderlineid,
res.replacedItemNumber,
paymentType_OM,
SAFE_CAST(NULL AS STRING) AS orderComments,
res.order_quantity,
res.sumnet
FROM (
SELECT  
IFNULL(o.combiordernumber, o.ordernumber) AS combiordernumber
, o.ordernumber
, CASE WHEN o.hakOrder THEN 'HAK' ELSE o.fulfillmenttype END AS  fulfillmenttype
, o.fulfillingstoreid
, o.customerHomeStoreNumber
, o.customerNumber
, o.cardholderid
, o.deliverydate
, 'PARCEL' AS deliverytype
, CASE WHEN employeeId = '' THEN NULL ELSE employeeId END AS validated_order_employee
, oItem.articleId
, COALESCE(oItem.positionNumber,pos+1) as positionNumber
, '' AS merchantname
, '' AS orderlineid
, oItem.replacedItemNumber
, oItem.quantity as order_quantity
, SAFE_CAST(oItem.priceData.netPrice.sumPrice.amount as FLOAT64) as sumnet
, MIN(payment.paymentType) as paymentType_OM
, MIN(o.origin.id) AS origin_id
, MIN(o.combiOrderId) AS combiorderid
, MIN(TIMESTAMP(FORMAT_TIMESTAMP('%F %T',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',creationTime)),'Europe/Paris'))) AS creationtime
FROM (
SELECT 
r.ordernumber
, r.combiordernumber
, r.status
, r.fulfillingStoreNumber
, r.revision
, r.dana_ingestion_timestamp
, RANK() over (PARTITION BY r.combiordernumber, r.ordernumber ORDER BY r.revision ASC, r.dana_ingestion_timestamp ASC) AS rnk
FROM `metro-bi-dl-fra-prod.ingest_om.fsd_v1` r
WHERE PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',creationTime)),'Europe/Paris'),FORMAT_TIMESTAMP('%F', dana_ingestion_timestamp, 'Europe/Paris'))) >= '2018-06-20'
AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
AND PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',creationtime)),'Europe/Paris')) {{ params.pattern_date }} 
	
) oMin 
JOIN `metro-bi-dl-fra-prod.ingest_om.fsd_v1` o 
ON oMin.ordernumber = o.ordernumber
AND COALESCE(oMin.combiordernumber,'') = COALESCE(o.combiordernumber,'')
AND oMin.status = o.status
AND oMin.fulfillingStoreNumber = o.fulfillingStoreNumber
AND oMin.revision = o.revision
AND oMin.dana_ingestion_timestamp = o.dana_ingestion_timestamp
AND oMin.rnk = 1
LEFT JOIN o.items as oItem WITH OFFSET pos
WHERE PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',o.creationTime)),'Europe/Paris'),FORMAT_TIMESTAMP('%F', o.dana_ingestion_timestamp, 'Europe/Paris'))) >= '2018-06-20'
AND DATE_ADD(DATE(o.PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
AND PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',o.creationtime)),'Europe/Paris'),FORMAT_TIMESTAMP('%F', o.dana_ingestion_timestamp, 'Europe/Paris'))) {{ params.pattern_date }} 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) res
WHERE res.replaceditemnumber IS NULL
),
  
ordered_frais_liv as (
SELECT combiorderid,
	origin_id,
	combiordernumber,
	ordernumber ,
	fulfillmenttype ,
	fulfillingstoreid,
	customerhomestorenumber,
	customernumber ,
	cardholderid,
	deliveryDate,
	deliveryType,
	pickupStoreid,
	validated_order_employee,
	COALESCE(creationtime, dana_ingestion_timestamp_min) AS creationtime,
	articleid,
	positionNumber,
	merchantname,
	orderlineid,
	replaceditemnumber,
	paymentType_OM,
	orderComments,
	order_quantity,
	sumNet
	FROM (
---- FRAIS LIV WEBSHOP
SELECT DISTINCT
	combiorderid,
	orderContext.origin.id AS origin_id,
	combiordernumber,
	ordernumber ,
	fulfillmenttype ,
	fulfillingstoreid,
	customerhomestorenumber,
	customernumber ,
	cardholdernumber AS cardholderid,
	deliveryinformation.deliveryDate,
	deliveryinformation.deliveryType,
	SAFE_CAST(deliveryinformation.pickupStoreid AS INT64) AS pickupStoreid,
	ordercontext.employeeId as validated_order_employee,
	TIMESTAMP(FORMAT_TIMESTAMP('%F %T',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',orderContext.creationTime)),'Europe/Paris')) AS creationtime,
	'DUMMY-FRAIS_LIV' articleid,
	null positionNumber,
	'' merchantname,
	'' as orderlineid,
	null as replaceditemnumber,
	1 as order_quantity,
	round((CASE WHEN  dana_ingestion_timestamp < '2019-10-01' then (pricesummary.sumNetInclTaxesFeesEmpties - priceSummary.sumNetInclTaxesAndFees) else pricesummary.sumDeliveryFees END),2) as sumNet,
	TIMESTAMP(FORMAT_TIMESTAMP('%F %T', dana_ingestion_timestamp, 'Europe/Paris')) AS dana_ingestion_timestamp_min,
	paymentData.paymentType as paymentType_OM,
	SAFE_CAST(NULL AS STRING) AS orderComments,
	RANK() OVER (PARTITION BY ordernumber ORDER BY dana_ingestion_timestamp ASC, paymentData.paymentType DESC) rank
FROM `metro-bi-dl-fra-prod.ingest_om.webshop`
WHERE DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
	AND PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ', orderContext.creationTime)),'Europe/Paris')) {{ params.pattern_date }}
	AND (pricesummary.sumNetInclTaxesFeesEmpties <> priceSummary.sumNetInclTaxesAndFees OR pricesummary.sumDeliveryFees <> 0)
UNION DISTINCT
---- FRAIS LIV DROPSHIP
SELECT DISTINCT
	combiorderid,
	orderContext.origin.id AS origin_id,
	combiordernumber,
	ordernumber ,
	fulfillmenttype ,
	fulfillingstoreid,
	customerhomestorenumber,
	customernumber ,
	cardholdernumber AS cardholderid,
	deliveryinformation.deliveryDate,
	deliveryinformation.deliveryType,
	SAFE_CAST(deliveryinformation.pickupStoreid AS INT64) AS pickupStoreid,
	ordercontext.employeeId as validated_order_employee,
	TIMESTAMP(FORMAT_TIMESTAMP('%F %T',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',orderContext.creationTime)),'Europe/Paris')) AS creationtime,
	'DUMMY-FRAIS_LIV' articleid,
	null positionNumber,
	'' merchantname,
	'' as orderlineid,
	null as replaceditemnumber,
	1 as order_quantity,
	round((CASE WHEN  dana_ingestion_timestamp < '2019-10-01' then (pricesummary.sumNetInclTaxesFeesEmpties - priceSummary.sumNetInclTaxesAndFees) else pricesummary.sumDeliveryFees END),2) as sumNet,
	TIMESTAMP(FORMAT_TIMESTAMP('%F %T', dana_ingestion_timestamp, 'Europe/Paris')) AS dana_ingestion_timestamp_min,
	paymentData.paymentType as paymentType_OM,
	SAFE_CAST(NULL AS STRING) AS orderComments,
	RANK() OVER (PARTITION BY ordernumber ORDER BY dana_ingestion_timestamp ASC, paymentData.paymentType DESC) rank
FROM `metro-bi-dl-fra-prod.ingest_om.dropship`
WHERE DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
	AND PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ', orderContext.creationTime)),'Europe/Paris')) {{ params.pattern_date }}
	AND (pricesummary.sumNetInclTaxesFeesEmpties <> priceSummary.sumNetInclTaxesAndFees OR pricesummary.sumDeliveryFees <> 0)
UNION DISTINCT
---- FRAIS LIV MARKETPLACE
SELECT DISTINCT
	combiorderid,
	orderContext.id AS origin_id,
	combiordernumber,
	ordernumber ,
	fulfillmenttype ,
	fulfillingstoreid,
	customerhomestorenumber,
	customernumber ,
	cardholdernumber AS cardholderid,
	delivery.Date,
	delivery.Type,
	SAFE_CAST(pickup.storeid AS INT64) AS pickupStoreid,
	ordercontext.employeeId as validated_order_employee,
	TIMESTAMP(FORMAT_TIMESTAMP('%F %T',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris')) AS creationtime,
	'DUMMY-FRAIS_LIV_MKP' articleid,
	null positionNumber,
	merchantname,
	'' as orderlineid,
	null as replaceditemnumber,
	1 as order_quantity,
	priceSummary.sumDeliveryFees as sumNet,
	TIMESTAMP(FORMAT_TIMESTAMP('%F %T', dana_ingestion_timestamp, 'Europe/Paris')) AS dana_ingestion_timestamp_min,
	payment.Type as paymentType_OM,
	SAFE_CAST(NULL AS STRING) AS orderComments,
	RANK() OVER (PARTITION BY ordernumber ORDER BY dana_ingestion_timestamp ASC, payment.Type DESC) rank
FROM `metro-bi-dl-fra-prod.ingest_om.marketplace` LEFT JOIN UNNEST(items) as item
WHERE DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
	AND PARSE_DATE('%F',COALESCE(FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris'),FORMAT_TIMESTAMP('%F', dana_ingestion_timestamp, 'Europe/Paris'))) {{ params.pattern_date }}
	and priceSummary.sumDeliveryFees <> 0
	)
WHERE rank = 1
),

mirakl_com as (
SELECT DISTINCT
  mkp_ol.*
FROM (
  SELECT
    OrderId,
    OrderLineId,
    MAX(dana_ingestion_timestamp) dana_ingestion_timestamp_max
  FROM
    `metro-bi-dl-fra-pp.ingest_fra.mkp_orderline`
  WHERE
    DATE(dana_ingestion_timestamp) {{ params.pattern_date }}
  GROUP BY
    1,
    2) mkp_olmax
JOIN (
  SELECT
    orderid,
    orderlineid,
    CommissionFee,
    Shopid,
    dana_ingestion_timestamp
  FROM
    `metro-bi-dl-fra-pp.ingest_fra.mkp_orderline`
  WHERE
    DATE(dana_ingestion_timestamp) {{ params.pattern_date }}) mkp_ol
ON
  ( mkp_ol.OrderId = mkp_olmax.OrderId
    AND mkp_ol.OrderLineId = mkp_olmax.OrderLineId
    AND mkp_ol.dana_ingestion_timestamp = mkp_olmax.dana_ingestion_timestamp_max)
),

emp_commission as (
SELECT
  combiorderid,
  bundleId,
  MIN(userid) userid
FROM (
  SELECT
    DISTINCT
    CASE
      WHEN dana_ingestion_timestamp < '2019-02-06' THEN SUBSTR(JSON_EXTRACT(tags, '$.orderId'), 2, LENGTH(JSON_EXTRACT(tags, '$.orderId'))-2)
      WHEN dana_ingestion_timestamp = '2019-02-06' THEN COALESCE(SUBSTR(JSON_EXTRACT(tags,'$.orderId'), 2, LENGTH(JSON_EXTRACT(tags,'$.orderId'))-2),parsedtags.orderId)
    ELSE parsedtags.orderid END AS combiorderid,
    CASE 
	  WHEN dana_ingestion_timestamp < '2019-07-10' THEN SUBSTR(JSON_EXTRACT(tags,'$.bundleId'), 2, LENGTH(JSON_EXTRACT(tags,'$.bundleId'))-2)
      WHEN dana_ingestion_timestamp = '2019-07-10' THEN COALESCE(SUBSTR(JSON_EXTRACT(tags,'$.bundleId'), 2, LENGTH(JSON_EXTRACT(tags,'$.bundleId'))-2),parsedtags.bundleid)
    ELSE parsedtags.bundleid END AS bundleid,
    SUBSTR(JSON_EXTRACT(tags,'$.userId'),2,LENGTH(JSON_EXTRACT(tags,'$.userId'))-2) userId
  FROM
    `metro-bi-dl-fra-prod.ingest_boc.structured`
  WHERE
    DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
    AND eventtype IN ('order-submit-employee-commissions.quantity', 'order-submit-employee-commissions.price', 'order.submit.employeeCommissions.price', 'order.submit.employeeCommissions.quantity')
    AND JSON_EXTRACT(tags,'$.userId') LIKE '%@%'
    AND (parsedtags.orderId IS NOT NULL OR JSON_EXTRACT(tags,'$.orderId') IS NOT NULL)
  UNION DISTINCT
  SELECT
    DISTINCT JSON_EXTRACT(tags,'$.orderId') combiorderid,
    JSON_EXTRACT(tags,'$.bundleId') bundleId,
    JSON_EXTRACT(tags,'$.userId') userId
  FROM
    `metro-bi-dl-fra-pp.ingest_fra.commissions`
  WHERE
    DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
    AND eventtype IN ('order-submit-employee-commissions.quantity','order-submit-employee-commissions.price')
    AND JSON_EXTRACT(tags,'$.userId') LIKE '%@%'
    AND JSON_EXTRACT(tags,'$.orderId') IS NOT NULL)
GROUP BY
  combiorderid,
  bundleid
),

order_count_emp_OM as (
SELECT 
ordernumber,
count(DISTINCT employee) as count_emp,
Max(employee) as employee_id FROM (
	SELECT DISTINCT
	ordernumber,
	ordercontext.employeeId as employee
	FROM `metro-bi-dl-fra-prod.ingest_om.webshop`
	WHERE
      PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',orderContext.creationTime)),'Europe/Paris')) >= '2018-06-20'
      AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
      AND PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',orderContext.creationTime)),'Europe/Paris')) {{ params.pattern_date }}
	UNION DISTINCT 
	SELECT DISTINCT
	ordernumber,
	ordercontext.employeeId as employee
	FROM `metro-bi-dl-fra-prod.ingest_om.dropship`
	WHERE
      PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',orderContext.creationTime)),'Europe/Paris')) >= '2018-06-20'
      AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
      AND PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',orderContext.creationTime)),'Europe/Paris')) {{ params.pattern_date }}
	UNION DISTINCT 
	SELECT DISTINCT
	ordernumber,
	CASE WHEN employeeId = '' THEN NULL ELSE employeeId END as employee
	FROM `metro-bi-dl-fra-prod.ingest_om.fsd_v1`
	WHERE
      PARTITIONTIME >= TIMESTAMP("2018-06-20")
      AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
      AND PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',creationTime)),'Europe/Paris')) {{ params.pattern_date }}
	UNION DISTINCT 
	SELECT DISTINCT
	ordernumber,
	employeeId as employee
	FROM `metro-bi-dl-fra-prod.ingest_om.marketplace`
	WHERE
      PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris')) >= '2018-06-20'
      AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
      AND PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris')) {{ params.pattern_date }}
	)
	group by ordernumber
),

order_count_emp_OM_and_BOC as (
SELECT 
combiorderid,
count(DISTINCT employee) as count_emp,
Max(employee) as employee_id FROM (
	SELECT DISTINCT
	combiorderid,
	ordercontext.employeeId as employee
	FROM `metro-bi-dl-fra-prod.ingest_om.webshop`
	WHERE
      PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',orderContext.creationTime)),'Europe/Paris')) >= '2018-06-20'
      AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
      AND PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',orderContext.creationTime)),'Europe/Paris')) {{ params.pattern_date }}
	UNION DISTINCT 
	SELECT DISTINCT
	combiorderid,
	ordercontext.employeeId as employee
	FROM `metro-bi-dl-fra-prod.ingest_om.dropship`
	WHERE
      PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',orderContext.creationTime)),'Europe/Paris')) >= '2018-06-20'
      AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
      AND PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',orderContext.creationTime)),'Europe/Paris')) {{ params.pattern_date }}
	UNION DISTINCT 
	SELECT DISTINCT
	combiorderid,
	employeeId as employee
	FROM `metro-bi-dl-fra-prod.ingest_om.fsd_v1`
	WHERE
      PARTITIONTIME >= TIMESTAMP("2018-06-20")
      AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
      AND PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',creationTime)),'Europe/Paris')) {{ params.pattern_date }}
	UNION DISTINCT 
	SELECT DISTINCT
	combiorderid,
	CASE WHEN employeeId = '' THEN NULL ELSE employeeId END as employee
	FROM `metro-bi-dl-fra-prod.ingest_om.marketplace`
	WHERE
      PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris')) >= '2018-06-20'
      AND DATE_ADD(DATE(PARTITIONTIME), INTERVAL 1 DAY) {{ params.pattern_date }}
      AND PARSE_DATE('%F',FORMAT_TIMESTAMP('%F',TIMESTAMP(PARSE_DATETIME('%FT%R:%E3SZ',IFNULL(orderContext.creationTime, creationtime))),'Europe/Paris')) {{ params.pattern_date }}
	UNION DISTINCT
	SELECT DISTINCT
	combiorderid,
	userid AS employee
	FROM emp_commission
	)
	group by combiorderid
),

ordered_info_emp AS (
SELECT 
DATE(new_target_data.creationtime) AS order_date,
SAFE_CAST(FORMAT_TIMESTAMP('%Y%m%d',new_target_data.creationtime) AS INT64) AS order_day,
SAFE_CAST(FORMAT_TIMESTAMP('%Y%m',new_target_data.creationtime) AS INT64) AS order_month,
FORMAT_TIMESTAMP('%R',new_target_data.creationtime) AS order_time,
new_target_data.origin_id,
new_target_data.combiordernumber,
new_target_data.ordernumber,
new_target_data.fulfillmenttype,
SAFE_CAST(new_target_data.fulfillingstoreid AS INT64) fulfillingstoreid,
SAFE_CAST(new_target_data.customerhomestorenumber AS INT64) customerhomestorenumber,
SAFE_CAST(new_target_data.customernumber AS INT64) customernumber,
SAFE_CAST(new_target_data.cardholderid AS INT64) cardholderid,
new_target_data.deliveryDate,
new_target_data.deliveryType,
new_target_data.pickupStoreid,
new_target_data.validated_order_employee,
emp_commission.userid AS added_to_basket_employee,
CASE WHEN (new_target_data.articleId like 'DUMMY%' AND order_count_emp_OM_and_BOC.count_emp >= 2) THEN null 
	 WHEN emp_commission.userid IS NOT NULL THEN emp_commission.userid 
	 WHEN new_target_data.validated_order_employee IS NOT NULL THEN new_target_data.validated_order_employee  
	 WHEN order_count_emp_OM.count_emp = 1 THEN order_count_emp_OM.employee_id 
	 ELSE emp_commission.userid END  AS affected_employee,
new_target_data.articleId,
MAX(SAFE_CAST(new_target_data.positionNumber AS INT64)) AS positionNumber,
mirakl_com.shopid,
new_target_data.merchantname,
new_target_data.paymentType_OM,
new_target_data.orderComments,
MAX(new_target_data.replaceditemnumber) AS replaceditemnumber,
ROUND(SUM(new_target_data.order_quantity),2) AS order_quantity,
ROUND(SUM(new_target_data.sumNet),2) AS order_article_net_price
FROM (SELECT * FROM new_target_data UNION ALL SELECT * FROM ordered_frais_liv) new_target_data
LEFT JOIN order_count_emp_OM_and_BOC
ON new_target_data.combiorderid = order_count_emp_OM_and_BOC.combiOrderId
LEFT JOIN order_count_emp_OM
ON new_target_data.ordernumber = order_count_emp_OM.ordernumber
LEFT JOIN emp_commission
ON new_target_data.combiorderid = emp_commission.combiorderid
and new_target_data.articleid = emp_commission.bundleid
LEFT JOIN mirakl_com
ON new_target_data.orderlineid = mirakl_com.orderlineid
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,21,22,23,24
),

ordered_rejects as (
SELECT 
rejects.order_date,
rejects.order_day,
rejects.order_month,
rejects.order_time,
rejects.origin_id,
rejects.combiordernumber,
rejects.ordernumber,
rejects.fulfillmenttype,
rejects.fulfillingstoreid,
rejects.customerHomeStoreNumber,
rejects.customernumber,
rejects.cardholderid,
rejects.deliveryDate,
rejects.deliveryType,
rejects.pickupStoreid,
rejects.validated_order_employee,
rejects.added_to_basket_employee,
rejects.affected_employee,
rejects.articleid,
rejects.positionNumber,
rejects.shopid,
rejects.merchantname,
rejects.paymentType_OM,
rejects.orderComments,
rejects.replacedItemNumber,
rejects.id_options,
rejects.order_quantity,
rejects.order_article_net_price,
rejects.reject_insert
FROM `DSG.fact_order_article_rejects{{ params.pattern_full }}` rejects
LEFT JOIN ordered_info_emp
on rejects.ordernumber = ordered_info_emp.ordernumber
and rejects.articleId = ordered_info_emp.articleid
WHERE ordered_info_emp.articleid IS NULL),

ordered_info_tot as (
SELECT 
ordered_info_emp.order_date,
ordered_info_emp.order_day,
ordered_info_emp.order_month,
ordered_info_emp.order_time,
ordered_info_emp.origin_id,
ordered_info_emp.combiordernumber,
ordered_info_emp.ordernumber,
ordered_info_emp.fulfillmenttype,
ordered_info_emp.fulfillingstoreid,
ordered_info_emp.customerHomeStoreNumber,
ordered_info_emp.customernumber,
ordered_info_emp.cardholderid,
ordered_info_emp.deliveryDate,
ordered_info_emp.deliveryType,
ordered_info_emp.pickupStoreid,
ordered_info_emp.validated_order_employee,
ordered_info_emp.added_to_basket_employee,
CASE WHEN ordered_info_emp.affected_employee LIKE '%@METRONOM-EXTERNAL%'  THEN NULL
     WHEN ordered_info_emp.affected_employee LIKE '%@ASF.MADM.NET%' THEN NULL
	 ELSE ordered_info_emp.affected_employee 
	 END AS affected_employee,
ordered_info_emp.articleid,
ordered_info_emp.positionNumber,
ordered_info_emp.shopid,
ordered_info_emp.merchantname,
ordered_info_emp.paymentType_OM,
ordered_info_emp.orderComments,
ordered_info_emp.replacedItemNumber,
null as id_options,
ordered_info_emp.order_quantity,
ordered_info_emp.order_article_net_price,
null as reject_insert
FROM ordered_info_emp
UNION ALL
SELECT * FROM ordered_rejects),

options as (
SELECT
  combiordernumber,
  ordernumber,
  articleid,
  ARRAY_AGG(IFNULL(id_option,'')) AS id_options
FROM (
  SELECT
    DISTINCT combiordernumber,
    ordernumber,
    item.articleid,
    serviceoption.id AS id_option
  FROM
    `metro-bi-dl-fra-prod.ingest_om.webshop`
  LEFT JOIN
    UNNEST(items) AS item
  LEFT JOIN
    UNNEST(item.articledata.serviceoptions) AS serviceoption
    WHERE
    DATE(dana_ingestion_timestamp) {{ params.pattern_date }}
  UNION ALL
  SELECT
    DISTINCT combiordernumber,
    ordernumber,
    item.articleid,
    serviceoption.id AS id_option
  FROM
    `metro-bi-dl-fra-prod.ingest_om.dropship`
  LEFT JOIN
    UNNEST(items) AS item
  LEFT JOIN
    UNNEST(item.articledata.serviceoptions) AS serviceoption 
    WHERE
    DATE(dana_ingestion_timestamp) {{ params.pattern_date }}
    )
GROUP BY
  combiordernumber,
  ordernumber,
  articleid
)

SELECT
ordered_info_tot.order_date,
ordered_info_tot.order_day,
ordered_info_tot.order_month,
ordered_info_tot.order_time,
ordered_info_tot.origin_id,
ordered_info_tot.combiordernumber,
ordered_info_tot.ordernumber,
ordered_info_tot.fulfillmenttype,
ordered_info_tot.fulfillingstoreid,
ordered_info_tot.customerHomeStoreNumber,
ordered_info_tot.customernumber,
ordered_info_tot.cardholderid,
ordered_info_tot.deliveryDate,
ordered_info_tot.deliveryType,
ordered_info_tot.pickupstoreid,
ordered_info_tot.validated_order_employee,
ordered_info_tot.added_to_basket_employee,
ordered_info_tot.affected_employee,
ordered_info_tot.articleid,
ordered_info_tot.positionNumber,
ordered_info_tot.shopid,
CASE WHEN ordered_info_tot.merchantname = '' THEN null ELSE ordered_info_tot.merchantname END merchantname,
ordered_info_tot.paymentType_OM,
ordered_info_tot.orderComments,
ordered_info_tot.replacedItemNumber,
options.id_options,
ordered_info_tot.order_quantity,
ordered_info_tot.order_article_net_price,
ordered_info_tot.reject_insert,
FORMAT_TIMESTAMP('%F %T', CURRENT_TIMESTAMP, 'Europe/Paris') as gcp_insert
FROM ordered_info_tot
LEFT JOIN options
ON ordered_info_tot.combiordernumber = options.combiordernumber
AND ordered_info_tot.ordernumber = options.ordernumber
AND ordered_info_tot.articleid = options.articleid