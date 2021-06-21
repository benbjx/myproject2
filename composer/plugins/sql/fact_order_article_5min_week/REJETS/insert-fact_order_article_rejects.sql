CREATE OR REPLACE TABLE `DSG.fact_order_article_rejects{{ params.pattern_full }}` as
SELECT
tmp.order_date ,
tmp.order_day , 
tmp.order_month ,
tmp.order_time ,
tmp.origin_id,
tmp.combiordernumber , 
tmp.ordernumber , 
tmp.fulfillmenttype , 
tmp.fulfillingstoreid , 
tmp.customerhomestorenumber , 
tmp.customernumber , 
tmp.cardholderid ,
tmp.deliverydate ,
tmp.deliverytype , 
tmp.pickupStoreid,
tmp.validated_order_employee ,
tmp.added_to_basket_employee ,
tmp.affected_employee ,
tmp.articleId , 
tmp.positionNumber ,
tmp.shopid ,
tmp.merchantName ,
tmp.paymentType_OM,
tmp.orderComments,
tmp.replaceditemnumber ,
tmp.id_options,
tmp.order_quantity ,
tmp.order_article_net_price,
IFNULL(tmp.reject_insert,tmp.gcp_insert) reject_insert,
CASE WHEN dimart.articleid IS NULL THEN 'Missing article'
WHEN (tmp.affected_employee IS NOT NULL and emp.userprincipalname IS NULL) THEN 'Missing employee'
WHEN (fulfillmenttype = 'MARKKETPLACE' AND shopmerch.shopid IS NULL) THEN 'Missing shopid'
END AS comments,
tmp.gcp_insert
FROM `DSG.fact_order_article_tmp{{ params.pattern_full }}` tmp
LEFT JOIN `DWH.dim_art_bundle` dimart
ON tmp.articleid = dimart.articleid
LEFT JOIN `DWH.dim_employee_histo` emp
ON tmp.affected_employee = emp.userprincipalname
AND tmp.order_date = emp.date_of_day
LEFT JOIN `DWH.bridge_shopid_merchantname` AS shopmerch
USING(merchantname)
WHERE dimart.articleid IS NULL 
OR (tmp.affected_employee IS NOT NULL AND emp.userprincipalname IS NULL)
OR (fulfillmenttype = 'MARKKETPLACE' AND shopmerch.shopid IS NULL)