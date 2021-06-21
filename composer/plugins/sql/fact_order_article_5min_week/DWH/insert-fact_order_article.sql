INSERT INTO `DWH.fact_order_article`

WITH old_target_data as (
SELECT ordernumber, articleid, positionnumber FROM `DWH.fact_order_article`
WHERE DATE_ADD(order_date, INTERVAL 2 DAY) {{ params.pattern_date }}
AND type_cmd IN (1,2) -- Vraies commandes et reliquats
),

-- liste des pk_article qui correspondent a des garanties
guarantee_article_param AS (
SELECT
  a.articleId
, a.combinedarticleid
, a.articleNumber
, a.pk_article
FROM `DWH.dim_art_bundle` a
WHERE a.articleNumber IN (
 156701,163323,650850,163326,163360,650924,163350,163333,163361,687636,156698,
 163342,687640,163349,156707,680837,163367,163336,666796,163357,163345,--624097,
 163362,156695,163341,163325,652119,163364,156692,687638,163332,666235,163337,
 163329,163352,163355,156697,156700,650866,163330,163338,163356,163343,652132,
 666798,163351,650908,163321,163368,156702,163346,163359,157090,687639,650847,
 156705,156696,163344,163369,163348,156706,163353,163366,163331,163328,163347,
 666260,156699,650851,163354,163339,156704,163334,156703,650848,163363,
 /* nouvelles ref webshop */
 827176,827177,827178,827179,827180,827933,827939,827940,827942,827944,827955,827950,
 827961,827953,827954,827962,827963,827964,827965,827966,827967,827968,827969,828012,
 828013,828014,828015,828017,828018,828019,828020,828021,828022,828023,828024,828025,
 828026,828027,828028,828029,828030,828031,828032,828036,828035,
 /* ref oubliees */
 163358,650852,650853,650855,650857,650858,650892,650893,650896,650897,650899,650905,652126,652127,652130,652135,666263,666264,680828,680835,680838,681270)
),

-- on calcul le nombre de garanties achetees dans chaque commande
guarantees_ordered AS (
SELECT o.order_date, o.combiordernumber, o.ordernumber, sum(order_qty_guarantee) order_qty_guarantee FROM 
(SELECT 
	  order_date
	, combiordernumber
	, ordernumber
	, articleId
	, order_quantity order_qty_guarantee
FROM `DSG.fact_order_article_tmp{{ params.pattern_full }}`
	WHERE fulfillingstoreid = 113
) o
JOIN guarantee_article_param a 
ON a.articleId = o.articleId
GROUP BY 1,2,3
),

-- liste des articles soumis a garantie = liste des articles pour lesquels une garantie est proposee
article_with_guarantee_proposed AS (
SELECT 
  distinct
  o.order_date
, o.combiordernumber
, o.ordernumber
, o.affected_employee
, o.articleId 
, o.order_quantity
FROM `DSG.fact_order_article_tmp{{ params.pattern_full }}` o
JOIN o.id_options op 
JOIN guarantee_article_param g ON (
    g.combinedarticleid = SAFE_CAST(op AS INT64))
),

guarantee_sold AS (
SELECT DISTINCT
  p.order_date
, p.combiordernumber
, p.ordernumber
, p.affected_employee
, p.articleId 
, p.order_quantity
, s.order_qty_guarantee
, sum(s.order_qty_guarantee) over (PARTITION BY   p.order_date,p.combiordernumber,p.ordernumber) AS qty_order_tot
, sum(s.order_qty_guarantee) over (PARTITION BY   p.order_date,p.combiordernumber,p.ordernumber,p.affected_employee) AS qty_order_emp_tot
, count(1) over (PARTITION BY   p.order_date,p.combiordernumber,p.ordernumber,p.affected_employee) AS count_order_line
FROM article_with_guarantee_proposed p
LEFT JOIN guarantees_ordered s ON
p.order_date = s.order_date
AND p.combiordernumber = s.combiordernumber
AND p.ordernumber = s.ordernumber
),

promo AS (
SELECT
date_of_day
, articleId
, store_id
, MAX(prom_id) AS prom_id
FROM `DWH.dim_promo`
WHERE date_of_day {{ params.pattern_date }}
AND articleid IS NOT NULL
GROUP BY 1,2,3
),

-- premier resultat des commandes, il ne reste plus qu'a corriger les affectations garantie
results_order AS (
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
emp.id_store store_emp ,
pref.store_id store_pref ,
CASE WHEN IFNULL(emp.service, 'CUSTOMER') = 'STORE' AND emp.id_store NOT IN (0,91,93,147,200) AND emp.id_store IS NOT NULL THEN emp.id_store WHEN pref.store_id IS NOT NULL THEN pref.store_id WHEN customerhomestorenumber = 147 THEN 32 ELSE customerhomestorenumber END store_cdg ,
IFNULL(emp.service, 'CUSTOMER') service ,
tmp.deliverydate ,
tmp.deliverytype ,
tmp.pickupStoreid,
tmp.validated_order_employee ,
tmp.added_to_basket_employee ,
tmp.affected_employee ,
tmp.articleId , 
dimart.combinedarticleid,
dimart.pk_article,
tmp.positionNumber ,
COALESCE(tmp.shopid, shopmerch.shopid) AS shopid ,
tmp.merchantName ,
tmp.paymentType_OM,
tmp.orderComments,
tmp.replaceditemnumber ,
tmp.id_options,
tmp.order_quantity ,
tmp.order_article_net_price,
promo.prom_id,
CASE WHEN ARRAY_LENGTH(SPLIT(tmp.ordernumber,'-')) > 2 THEN 2
	 ELSE 1 END AS type_cmd,
g.order_qty_guarantee
FROM `DSG.fact_order_article_tmp{{ params.pattern_full }}` tmp
LEFT JOIN guarantees_ordered g
ON g.order_date = tmp.order_date
AND g.combiordernumber = tmp.combiordernumber
AND g.ordernumber = tmp.ordernumber
LEFT JOIN `DWH.dim_art_bundle` dimart
ON tmp.articleid = dimart.articleid
LEFT JOIN old_target_data
ON tmp.ordernumber = old_target_data.ordernumber
AND tmp.articleid = old_target_data.articleid
LEFT JOIN (SELECT DISTINCT home_store_id, cust_no, month_id, store_id FROM `DRE.mdw_store_pref_all`) pref
ON (tmp.customerhomestorenumber = pref.home_store_id AND tmp.customernumber = pref.cust_no AND tmp.order_month = pref.month_id)
LEFT JOIN `DWH.dim_employee_histo` emp
ON tmp.affected_employee = emp.userprincipalname
AND tmp.order_date = emp.date_of_day
LEFT JOIN promo
ON tmp.articleid = promo.articleid
AND tmp.order_date = promo.date_of_day
AND tmp.fulfillingstoreid = promo.store_id
LEFT JOIN `DWH.bridge_shopid_merchantname` AS shopmerch
USING(merchantname)
WHERE dimart.articleid IS NOT NULL 
AND (tmp.affected_employee IS NULL OR emp.userprincipalname IS NOT NULL)
AND (fulfillmenttype <> 'MARKKETPLACE' OR shopmerch.shopid IS NOT NULL)
AND old_target_data.ordernumber IS NULL
),

-- liste des commandes, articles avec leurs garanties proposees
perim_order AS (
SELECT * FROM (
SELECT 
order_date
, ordernumber
, SAFE_CAST(id_option AS INT64) id_option 
, combinedarticleid
, pk_article
, order_quantity
, order_qty_guarantee
, affected_employee
FROM results_order LEFT JOIN UNNEST(id_options) AS id_option
WHERE type_cmd = 1
)
WHERE id_option IS NOT NULL
),

-- liste des garanties commandees
perim_order_guar AS (
SELECT 
order_date
, ordernumber
, results_order.combinedarticleid
, pk_article
, order_quantity
, affected_employee
FROM results_order
JOIN guarantee_article_param 
USING(pk_article)
WHERE type_cmd = 1
),

-- liste des articles (hors garantie) appartenant a une commande dans laquelle au moins une garantie a ete achetee
perim_article_under_guar AS (
SELECT perim_order.* FROM perim_order 
LEFT JOIN guarantee_article_param
USING(pk_article)
WHERE IFNULL(perim_order.order_qty_guarantee ,0) <> 0
AND guarantee_article_param.pk_article IS NULL
),


-- jointure : on prend la liste des garanties commandees jointe aux articles soumis a garantie pour recuperer l employe affecte
correction_affectation_gar_emp AS (
SELECT * EXCEPT(rnk,affected_employee,combinedarticleid ) FROM (
SELECT 
perim_order_guar.*
, perim_article_under_guar.affected_employee AS affected_employee_corrected
--, perim_article_under_guar.pk_article AS pk_article_affected
, RANK() OVER(PARTITION BY perim_order_guar.ordernumber, perim_order_guar.pk_article ORDER BY perim_article_under_guar.combinedarticleid, perim_article_under_guar.affected_employee DESC) rnk
FROM perim_order_guar 
LEFT JOIN perim_article_under_guar
ON perim_order_guar.ordernumber = perim_article_under_guar.ordernumber
AND perim_order_guar.combinedarticleid = perim_article_under_guar.id_option
)
WHERE affected_employee <> affected_employee_corrected
AND rnk = 1
),

--- quantite totale de garanties commandees
qty_guarantee_ordered_tot AS (
SELECT
ordernumber
, SUM(order_quantity) AS qty_guar_tot
FROM perim_order_guar
GROUP BY 1
),


affectation_article_gar AS (
--- on s'assure que le nombre de garanties affectees a chaque article ne depasse pas le nombre de garanties disponibles
SELECT
o.* EXCEPT(max_gar_possible)
, CASE WHEN (IFNULL(qty_guarantee_ordered_tot.qty_guar_tot,0) - SUM(max_gar_possible) OVER (PARTITION BY ordernumber ORDER BY pk_article RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) >= 0 THEN max_gar_possible
	   ELSE 0 
	   END AS qty_guarantee_adjusted_sold
FROM (
--- on affecte a chaque article une garantie commandee et on determine le nombre max de garanties commandees possibles pour cet article
SELECT
perim_article_under_guar.order_date
, perim_article_under_guar.ordernumber
, perim_article_under_guar.pk_article
, MAX(CASE WHEN perim_order_guar.order_quantity - perim_article_under_guar.order_quantity >= 0 THEN perim_article_under_guar.order_quantity
	   ELSE 0 
	   END ) AS max_gar_possible
FROM perim_article_under_guar
JOIN perim_order_guar
ON perim_article_under_guar.ordernumber = perim_order_guar.ordernumber
AND perim_article_under_guar.id_option = perim_order_guar.combinedarticleid
GROUP BY 1,2,3
) o
JOIN qty_guarantee_ordered_tot
USING(ordernumber)
),

--- Reliquats : affectation des informations de commande origine

perim_order_orig AS (
SELECT
ordernumber
, order_date
, MIN(deliverydate) AS deliveryDate
FROM `DWH.fact_order_article` 
WHERE ARRAY_LENGTH(SPLIT(ordernumber, "-")) = 2
AND fulfillmenttype IN ('WEBSHOP', 'DROPSHIPMENT', 'CLICK AND COLLECT')
AND type_cmd = 1
AND order_date {{ params.pattern_date_long }}
GROUP BY 1,2
),

perim_order_article_orig AS (
SELECT 
ordernumber
, pk_article  
, affected_employee
, order_quantity
FROM `DWH.fact_order_article` 
WHERE ARRAY_LENGTH(SPLIT(ordernumber, "-")) = 2
AND fulfillmenttype IN ('WEBSHOP', 'DROPSHIPMENT', 'CLICK AND COLLECT')
AND type_cmd = 1
AND order_date {{ params.pattern_date_long }}
)

SELECT
results_order.order_date ,
results_order.order_day , 
results_order.order_month ,
results_order.order_time ,
results_order.origin_id,
results_order.combiordernumber , 
results_order.ordernumber , 
results_order.fulfillmenttype , 
results_order.fulfillingstoreid , 
CASE WHEN ARRAY_LENGTH(SPLIT(results_order.ordernumber, "-")) > 2 AND results_order.ordernumber LIKE '9-%' THEN perim_order_orig.order_date 
	 ELSE results_order.order_date
	 END AS order_date_orig,
CASE WHEN ARRAY_LENGTH(SPLIT(results_order.ordernumber, "-")) > 2 AND results_order.ordernumber LIKE '9-%' THEN perim_order_orig.ordernumber 
	 ELSE results_order.ordernumber
	 END AS ordernumber_orig,
CASE WHEN ARRAY_LENGTH(SPLIT(results_order.ordernumber, "-")) > 2 AND results_order.ordernumber LIKE '9-%' THEN perim_order_orig.deliverydate 
	 ELSE results_order.deliverydate
	 END AS deliverydate_orig,		
--CASE WHEN ARRAY_LENGTH(SPLIT(results_order.ordernumber, "-")) > 2 AND results_order.ordernumber LIKE '9-%' THEN CONCAT(perim_order_orig.ordernumber, perim_order_orig.positionNumber)
--	 ELSE CONCAT(results_order.ordernumber, results_order.positionNumber)
--	 END AS idCmdLigneOrig,	 
results_order.customerhomestorenumber , 
results_order.customernumber , 
results_order.cardholderid ,
results_order.store_emp ,
results_order.store_pref ,
results_order.store_cdg ,
results_order.service ,
results_order.deliverydate ,
results_order.deliverytype , 
results_order.pickupStoreid,
results_order.validated_order_employee ,
results_order.added_to_basket_employee ,
CASE WHEN ARRAY_LENGTH(SPLIT(results_order.ordernumber, "-")) > 2 AND perim_order_article_orig.pk_article IS NOT NULL THEN perim_order_article_orig.affected_employee
	WHEN results_order.order_date < '2020-09-28' THEN results_order.affected_employee 
	ELSE IFNULL(correction_affectation_gar_emp.affected_employee_corrected, results_order.affected_employee)
	END AS affected_employee,
results_order.articleId , 
results_order.combinedarticleid,
results_order.pk_article,
results_order.positionNumber ,
results_order.shopid ,
results_order.merchantName ,
results_order.paymentType_OM,
results_order.orderComments,
results_order.replaceditemnumber ,
results_order.id_options,
CASE WHEN ARRAY_LENGTH(SPLIT(results_order.ordernumber, "-")) > 2 AND results_order.ordernumber LIKE '9-%' THEN 0 
	 ELSE results_order.order_quantity
	 END AS order_quantity,	
CASE WHEN ARRAY_LENGTH(SPLIT(results_order.ordernumber, "-")) > 2 AND results_order.ordernumber LIKE '9-%' THEN 0 
	 ELSE results_order.order_article_net_price
	 END AS order_article_net_price,	
CASE WHEN ARRAY_LENGTH(SPLIT(results_order.ordernumber, "-")) > 2 AND results_order.ordernumber LIKE '9-%'  THEN results_order.order_quantity
	 ELSE 0
	 END AS order_quantity_reliquat,
CASE WHEN ARRAY_LENGTH(SPLIT(results_order.ordernumber, "-")) > 2 AND results_order.ordernumber LIKE '9-%'  THEN results_order.order_article_net_price
	 ELSE 0
	 END AS order_article_net_price_reliquat,
CASE WHEN ARRAY_LENGTH(SPLIT(results_order.ordernumber, "-")) > 2 AND perim_order_article_orig.pk_article IS NULL THEN "Ajout"
	 WHEN ARRAY_LENGTH(SPLIT(results_order.ordernumber, "-")) > 2 AND perim_order_article_orig.pk_article IS NOT NULL AND perim_order_article_orig.order_quantity = results_order.order_quantity THEN "Reliquat"
	 WHEN ARRAY_LENGTH(SPLIT(results_order.ordernumber, "-")) > 2 AND perim_order_article_orig.pk_article IS NOT NULL AND perim_order_article_orig.order_quantity > results_order.order_quantity THEN "Minoration"
	 WHEN ARRAY_LENGTH(SPLIT(results_order.ordernumber, "-")) > 2 THEN "Autre" 
	 ELSE SAFE_CAST(NULL AS STRING)  
	 END AS type_reliquat,
results_order.prom_id,
results_order.type_cmd,
results_order.order_qty_guarantee AS qty_potential_guarantee_sold,
IFNULL(affectation_article_gar.qty_guarantee_adjusted_sold,0) AS qty_guarantee_adjusted_sold,
FORMAT_TIMESTAMP('%F %T', CURRENT_TIMESTAMP, 'Europe/Paris') AS gcp_insert
FROM results_order
LEFT JOIN correction_affectation_gar_emp
USING(order_date, ordernumber, pk_article)
LEFT JOIN affectation_article_gar
USING(order_date, ordernumber, pk_article)
LEFT JOIN perim_order_article_orig
ON CONCAT('9-', SPLIT(results_order.ordernumber, "-")[OFFSET(1)]) = perim_order_article_orig.ordernumber
AND results_order.pk_article = perim_order_article_orig.pk_article
LEFT JOIN perim_order_orig
ON CONCAT('9-', SPLIT(results_order.ordernumber, "-")[OFFSET(1)]) = perim_order_orig.ordernumber