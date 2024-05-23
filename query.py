class Query:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    def query_ordercount(self):
        query = f'''
            with arrival_guarantee as (SELECT 
            lc."name" AS "센터명",
            (case when os."originalOrderId" is not null then os."originalOrderId"
            else os."orderCode" end) AS "주문번호",
            os."originalProductOrderId" AS "보조주문번호",
            p."name" AS "고객사명",
            TO_CHAR(os."orderAt" at time zone 'Asia/Seoul', 'YYYY-MM-DD') AS "주문일자"
            FROM commerce."orderSheets" AS os
            	LEFT JOIN master.partners p ON p.id = os."partnerId"
            	LEFT JOIN logistics."logisticsCenters" lc ON lc.id = os."logisticsCenterId"
            WHERE 1=1
             --AND os."orderAt" AT TIME ZONE 'Asia/Seoul' >= '2024-05-12 00:00:00+09'::timestamp
             --AND os."orderAt" AT TIME ZONE 'Asia/Seoul' < '2024-05-13 00:00:00+09'::timestamp
             AND os."orderAt" AT TIME ZONE 'Asia/Seoul' >= '{self.start_date}  00:00:00+09'::timestamp
             AND os."orderAt" AT TIME ZONE 'Asia/Seoul' < '{self.end_date} 00:00:00+09'::timestamp             
             AnD lc."code" in ('ES1', 'ES2','ES3','LC1','PAJU')
             AND (os."deliveryAttributeType" = 'ARRIVAL_GUARANTEE' or os."deliveryAttributeType" = 'TODAY')
            ORDER BY lc."name") 
            select "주문일자","센터명","주문번호","보조주문번호","고객사명"
			--count(distinct "주문번호") as "출고건수"
            from arrival_guarantee
            --where "주문일자" between '2024-05-01' and '2024-05-02'   
            group by 1,2,3,4,5;
        '''
        return query
    def query_inoutrate(self):
        query = f'''
                select i."id", i."code" as "송장번호", i."closeAt" as "마감일시", 
                (case when os."originalOrderId" is not null 
                then os."originalOrderId" else os."orderCode" end) AS "주문번호",
                os."originalProductOrderId" as "보조주문번호", lc.name AS "센터명",
                p.name AS "고객사명", (CASE WHEN (i."deliverySheetId" IS NOT NULL) THEN 'Y'
                ELSE 'N' END) AS "B2B출고여부"
                from wms.operations op 
                LEFT JOIN logistics."invoices" i  ON op."invoiceId" = i. id
                left join commerce."orderSheets" os on op."orderSheetId" = os.id
                LEFT JOIN master.partners p ON p.id = op."partnerId"
                LEFT JOIN logistics."logisticsCenters" lc ON i."logisticsCenterId" = lc.id 
                where 1=1 
                --AND i."closeAt" AT TIME ZONE 'Asia/Seoul' >= '2024-05-12 04:00:00+09' AT TIME ZONE 'Asia/Seoul'
                --AND "i"."closeAt" AT TIME ZONE 'Asia/Seoul' < '2024-05-13 04:00:00+09'  AT TIME ZONE 'Asia/Seoul'
                AND i."closeAt" AT TIME ZONE 'Asia/Seoul' >= '{self.start_date} 04:00:00+09' AT TIME ZONE 'Asia/Seoul'
                AND "i"."closeAt" AT TIME ZONE 'Asia/Seoul' < '{self.end_date} 04:00:00+09' AT TIME ZONE 'Asia/Seoul'
                AnD lc."code" in ('ES1', 'ES2','ES3','LC1','PAJU') 
                ORDER BY i."closeAt" asc ;
                '''
        return query
        
    def query_monthlyrelease(self):
        query = f'''
        with add_month as (
            select 
                concat(
                    date_part('year', "마감일자_일자"::date),
                    '-',
                    case 
                        when date_part('month', "마감일자_일자"::date) >= 10 then null 
                        else '0' 
                    end,
                    date_part('month', "마감일자_일자"::date),
                    '-01'
                ) as "sales_month", 
                "마감일자", 
                "마감송장건수" as "sales" 
            from release.release_information	
            where 
                "출고일자" between '{self.start_date}' and '{self.end_date}'
        ), table_set as (
            select
                "sales_month",
                sum("sales") over (partition by "sales_month") AS "sales"
            from add_month
        ), retail_sales as (
            select 
                'category' as "category",
                "sales_month",
                "sales" 
            from table_set
        )
        select 
            a.sales_month as "월",
            '실제출고_물동량' as "구분",
            a.sales as "출고건수"
        from retail_sales a
        union all
        select 
            a.sales_month as "월",
            '3개월이동평균_물동량' as "구분",
            round(avg(b.sales),0) as "출고건수"
        from retail_sales a
        join retail_sales b on b."category" = a."category"
            and b.sales_month::date between a.sales_month::date - interval '2 months' and a.sales_month::date 
        group by a.sales_month
        order by 1;
        '''
        return query
#데이터 주문량 추출
    def query_orders(self):
        query = f'''WITH b2c_orders_cnt AS (
    SELECT
        TO_CHAR(os."orderAt" AT TIME ZONE 'Asia/Seoul', 'YYYY-MM-DD HH24:MI:SS') AS "주문일자",
        TO_CHAR(os."orderAt" AT TIME ZONE 'Asia/Seoul', 'YYYY-MM-DD') AS "주문일자_일자",
        TO_CHAR(os."orderAt" AT TIME ZONE 'Asia/Seoul', 'YYYY') AS "주문일자_연",
        TO_CHAR(os."orderAt" AT TIME ZONE 'Asia/Seoul', 'MM') AS "주문일자_월",
        TO_CHAR(os."orderAt" AT TIME ZONE 'Asia/Seoul', 'DD') AS "주문일자_일",
        TO_CHAR(os."orderAt" AT TIME ZONE 'Asia/Seoul', 'HH24') AS "주문일자_시",
        os."originalOrderId" as "주문번호", 
        c."name" AS "채널명",
        lc.name AS "센터명",
        p.name AS "고객사명"
	from commerce."orderSheets" os
		inner JOIN commerce.channels c ON c.id = os."channelId"
		inner JOIN master.partners p ON p.id = os."partnerId"
		inner JOIN logistics."logisticsCenters" lc ON lc.id = os."logisticsCenterId"
	where 1=1
   	    AND os."originalOrderId" is not null
        AND os."isCanceled" is False
		AND os."orderAt" AT TIME ZONE 'Asia/Seoul' >= '{self.start_date} 00:00:00+09'::timestamp
		AND os."orderAt" AT TIME ZONE 'Asia/Seoul' < '{self.end_date} 00:00:00+09'::timestamp
        AND lc.name NOT LIKE '%%해지%%'
        AND lc.name NOT LIKE '%%미사용%%'
), pro_orders as (SELECT
    "주문번호"
	,"주문일자"
	,"채널명"
    ,"센터명"
    ,"고객사명"
	,CONCAT("주문일자", ',',"주문일자_일자", ',', "주문일자_연", ',', "주문일자_월", ',',
	"주문일자_일", ',', "주문일자_시", ',', "주문번호", ',',"채널명", ',',"센터명",',', "고객사명") AS "information"
FROM
    b2c_orders_cnt
) select * from pro_orders
;
'''
        return query
    
#데이터 정합성 backend_invoices vs lms_invoices2
    def query_lms_invoice(self):
        query = 'select serial from invoices2 where serial is not null'
        return query
    
#출고에 관한 쿼리
    def query_release(self):
        query = f'''
          WITH b2c_invoice_cnt AS (
    SELECT
        TO_CHAR(i."closeAt" AT TIME ZONE 'Asia/Seoul', 'YYYY-MM-DD HH24:MI:SS') AS "마감일자",
        TO_CHAR(i."closeAt" AT TIME ZONE 'Asia/Seoul', 'YYYY-MM-DD') AS "마감일자_일자",
        TO_CHAR(i."closeAt" AT TIME ZONE 'Asia/Seoul', 'YYYY') AS "마감일자_연",
        TO_CHAR(i."closeAt" AT TIME ZONE 'Asia/Seoul', 'MM') AS "마감일자_월",
        TO_CHAR(i."closeAt" AT TIME ZONE 'Asia/Seoul', 'DD') AS "마감일자_일",
        TO_CHAR(i."closeAt" AT TIME ZONE 'Asia/Seoul', 'HH24') AS "마감일자_시",
        i."closeAt" AT TIME ZONE 'Asia/Seoul' AS "마감일시",
        i.code AS "송장번호",
        lc.name AS "센터명",
        p.name AS "고객사명",
        (CASE
            WHEN (i."deliverySheetId" IS NOT NULL) THEN 'Y'
            ELSE 'N'
        END) AS "B2B출고여부"
    FROM
        logistics."invoices" i
        LEFT JOIN master.partners p ON p.id = i."partnerId"
        LEFT JOIN logistics."logisticsCenters" lc ON lc.id = i."logisticsCenterId"
    WHERE
        i."tempOrderSheetId" IS NULL
        AND i."isConfirmed" = TRUE
        AND i."isClosed" = TRUE
        AND i."isCanceled" = FALSE
        AND i."isPostponed" = FALSE
        AND i."isAccepted" = TRUE
        AND i."closeAt" AT TIME ZONE 'Asia/Seoul' >= '{self.start_date} 04:00:00+09'::timestamp
        AND "i"."closeAt" AT TIME ZONE 'Asia/Seoul' < '{self.end_date} 04:00:00+09'::timestamp
        AND lc.name NOT LIKE '%%해지%%'
        AND lc.name NOT LIKE '%%미사용%%'
    ORDER BY
        i."closeAt" ASC
)
SELECT
    "송장번호"
    ,"센터명"
    ,"고객사명"
	,CONCAT("마감일자", ',',"마감일자_일자", ',', "마감일자_연", ',', "마감일자_월", ',', "마감일자_일", ',', "마감일자_시", ',', "송장번호", ',',"센터명",',', "고객사명") AS "information"
FROM
    b2c_invoice_cnt
ORDER BY
    "마감일자" ASC;
    '''
        return query
   
    def query_now_newb2b(self):
        query = f'''
        with test as (
        select 
        	to_char(bo.delivery_date at time zone 'Asia/Seoul', 'yyyy-mm-dd') as delivery_date,
        	to_char(bo.last_closed_at at time zone 'Asia/Seoul', 'yyyy-mm-dd') as close_date,
        	bo.partner_id, -- >> authentication-db & public-parters-tb & id,name - cols key, value
        	bo.center_id, -- >> backend-db & logistics.logisticsCenters & id, code, name - cols key, value1, value2
        	concat('DSBB','-',bo.partner_id, '-', to_char(bo.created_at at time zone 'Asia/Seoul', 'yymmdd'), '-', bo.id) as b2bcode,
        	ps."invoice_serial" as "송장번호",
        	bo.total_cost, --as "총비용",
        	bo.delivery_fare, --as "배송실비",
        	bo.handling_cost, --as "출고수수료비", 
        	bo.surcharges, --as "기타 수수료",
        	(case 
        		when bo.delivery_type = 'COUPANG_PALLET' then '쿠팡 밀크런 배차' 
        		when bo.delivery_type = 'B2B_PALLET' then '품고 배차' 
        		when bo.delivery_type = 'B2B_DELIVERY' then '품고 택배' 		
        	end) as delivyer_type,
        	(case 
        		when bo.packaging_type = 'BOX' then '박스' 
        		when bo.packaging_type = 'PALLET' then '팔레트' 	
        	end) as packaging_type,
        	(case when bo."is_exempted" is true then 'Y' else 'N' end) as "청구면제여부",
        	bo.id,
        	bo.delivery_date at time zone 'Asia/Seoul', -- as "출고(마감)일자", 
        	bo.brand_id,
        	bo.delivery_date 
        from 
        	b2b_orders bo
        	left join packlist as ps ON ps."b2b_order_id" = bo.id
        where 1=1 
        	and bo.status in ('DONE') -- 마감 
            --and bo.last_closed_at >= '2024-01-01 08:00:00+09' 
            --and bo.last_closed_at <  '2024-04-09 00:00:00+09'
        	and bo.created_at >= '{self.start_date}'
        	and bo.created_at <  '{self.end_date}'
            and ps."invoice_serial" is not null
        )
        select * from test-- where b2bcode = 'DSBB-107069-240404-6964'; 
        ;
        '''
        return query

    def query_back_oldb2b(self):
        query = f'''
        select
        	ds."code" AS "B2B출고번호",
        	li."code" as "송장번호",
        	lc."name" AS "센터명",
        	p."name" AS "고객사명",
        	c."name" as "플랫폼명",
        	ds."executeAt" AS "출고요청 마감일시",
        	li."closeAt" as "송장 마감일시",
        	ds."cost" AS "퀵실비",
        	ds."deliveryType" AS "배차유형",
        	ds."charges" AS "출고수수료",
        	ds."otherFees" AS "기타 수수료",
        	ds."otherFeesDescription" AS "기타 수수료 설명"
        from wms.operations op
             inner join logistics."deliverySheets" ds ON op."deliverySheetId" = ds.id
          AND op."isCanceled" IS FALSE
          AND op."isExecuted" IS TRUE
          AND ds."isCanceled" IS FALSE
          AND op."deliverySheetId" IS NOT NULL
        	and (ds."typeCodeKey" = 'DELIVERYSHEET_TYPE_QUICK'
        	or ds."typeCodeKey" ='DELIVERYSHEET_TYPE_KEEPING')
             JOIN master.partners p ON p.id = op."partnerId"
             LEFT OUTER JOIN commerce."channels" c ON c.id = ds."channelId"
        		 JOIN logistics."logisticsCenters" as lc ON lc.id = ds."logisticsCenterId"	
        		join logistics.invoices as li on li."deliverySheetId"  = ds.id
        where 1=1
        	and (ds."executeAt" at time zone 'Asia/Seoul' >= '{self.start_date}')
        	and (ds."executeAt" at time zone 'Asia/Seoul' < '{self.end_date}')
         ;
        '''
        return query
    
    def query_back_center(self):
        query = '''
        select id, code, name from logistics."logisticsCenters"
        ;
        '''
        return query


    def query_auth_partner(self):
        query = '''
        select id, name from partners
        ;
        '''
        return query

    def query_back_center_re(self):
        query = '''
        select id as centerid, code, name from logistics."logisticsCenters"
        ;
        '''
        return query

    def query_back_contact(self):
        query = '''
        select "lmsContractId", "logisticsPartnerId", "logisticsCenterId" 
        from logistics."logisticsContracts" lc
        ;
        '''
        return query
    
    def query_auth_partner_re(self):
        query = '''
        select id as partnerid, name from partners
        ;
        '''
        return query
        
#독특한 마감내역
    def query_back_close(self):
        query = f'''
        with close_b2c_invoices as (select
        "closeUser"."name" AS "마감자",
        lp."name" as "물류파트너",
        p."name" as "고객사명",
        lc."name" as "센터명",
        os."originalOrderId" AS "주문번호",
        os."originalProductOrderId" AS "상품별주문번호",
        i."code" AS "송장번호",
        i."code" AS "code",
        os."orderAt" at time zone 'Asia/Seoul' AS "주문일시",
        i."closeAt" at time zone 'Asia/Seoul' AS "마감일시",
        op."resourceData" ->> 'name' AS "품명",
        op."resourceData" ->> 'code' AS "바코드",
        op."doneQuantity" AS "수량",
        	br."code" AS "박스바코드",
        	br."name" AS "박스명"
        from logistics."invoices" i
        	left join wms.operations op ON op."invoiceId" = i.id
        	left join commerce."orderSheets" os ON os.id = op."orderSheetId"
        	LEFT JOIN master.partners p ON p.id = i."partnerId"
            LEFT JOIN commerce.channels c ON c.id = i. "channelId"
            LEFT JOIN master."brands" AS b ON b."id" = i."brandId"
            LEFT JOIN logistics."logisticsCenters" lc ON lc.id = i. "logisticsCenterId"
            LEFT JOIN logistics."logisticsPartners" lp ON lp.id = i. "logisticsPartnerId"
            LEFT OUTER JOIN "master"."users" AS "closeUser" ON "i"."closeUserId" = "closeUser"."id"
            LEFT OUTER JOIN "master"."codes" AS "cargoStatusCode" ON "i"."cargoStatusCodeKey" = "cargoStatusCode"."key"
        	LEFT JOIN master."resources" AS br ON br."id" = i."boxResourceId"	
        WHERE
          i. "tempOrderSheetId" IS null 
         AND "i"."isConfirmed" = true  
         and "i"."isClosed" = true     
         AND "i"."isCanceled" = false  
         AND "i"."isPostponed" = false 
         AND "i"."isAccepted" = true 
         AND i."closeAt" >= '{self.start_date}'
         AND i."closeAt" < '{self.end_date}'
         --AnD lc."code"='ES1'
         and lp."id" in (498)
         ) --
         select "송장번호", "고객사명", "센터명" from close_b2c_invoices; 
        '''
        return query
    def query_now_packlist(self):
        query = f'''
        select 
        ps."invoice_serial" as "code", 
        bo.center_id  as "centerid",
        bo.partner_id  as "partnerid"
        from b2b_orders bo 
       	left join packlist as ps ON ps."b2b_order_id" = bo.id
        where bo.created_at >= '{self.start_date}'
       	AND bo.created_at < '{self.end_date}'
       	and ps."invoice_serial" is not null
        ;
        '''
        return query
        
#반품에 관한 쿼리
    def query_now_return(self):
        query = f'''
        /*회수일시*/
        with close_return as (
        select
        to_char(cs.created_at at time zone 'Asia/Seoul', 'yyyy-mm-dd') as 생성일자,
        extract('HOUR' from cs.created_at at time zone 'Asia/Seoul') as 생성시간대,
        to_char(ri.done_at at time zone 'Asia/Seoul', 'yyyy-mm-dd') as 회수일자,
        extract('HOUR' from ri.done_at at time zone 'Asia/Seoul') as 회수시간대,
        cs."logistics_center_id" AS "centerid",
        cs."partner_id" AS "partnerid",
        ri."code" AS "송장번호",
        cs.created_at at time zone 'Asia/Seoul' as "생성일자", --cs에서 시스템에 등록하여 데이터가 생성된 일
        ri."done_at" AT time zone 'Asia/Seoul' AS "회수일시", -- 물품이 회수가 되어 시스템에 등록된 일
        ri."logistics_contract_id" as "lmsContractId"
        FROM cs AS cs
        	LEFT JOIN return_invoices AS ri ON cs."id" = ri."cs_id"
        WHERE 1=1
        AND cs.type in ('EXCHANGE','RETURN')
        AND ri."code" is not null
        and ri."done_at" >= {self.start_date}
        and ri."done_at" < {self.end_date}
        )
        select * from close_return
        ;
        '''
        return query