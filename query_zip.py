class Query:
    def query_invoice(self):
        query ='''
                select i."code", i."closeAt", i."deliverySheetId"
                ,i."partnerId" ,i."logisticsCenterId", i."brandId"
                ,i."channelId"
                from logistics."invoices" i
                where 1=1
                AND i."tempOrderSheetId" IS NULL
                AND i."isConfirmed" = TRUE
                AND i."isClosed" = TRUE
                AND i."isCanceled" = FALSE
                AND i."isPostponed" = FALSE
                AND i."isAccepted" = TRUE
                AND i."closeAt" AT TIME ZONE 'Asia/Seoul' >= '2022-01-01 04:00:00+09'::timestamp
                AND i."closeAt" AT TIME ZONE 'Asia/Seoul' < '2024-05-01 04:00:00+09'::timestamp
                ;
                '''
                query_partner = 'select p.id, p.name from master.partners p;'
                
                query_center = '''
                select lc.id, lc.name from logistics."logisticsCenters" lc
                where 1=1
                AND lc.name NOT LIKE '%%해지%%'
                AND lc.name NOT LIKE '%%미사용%%'
                '''
        return query
        
    def query_partner(self):
        query ='select p.id, p.name from master.partners p;'
        return query
        
    def query_center(self):
        query ='''
                select lc.id, lc.name from logistics."logisticsCenters" lc
                where 1=1
                AND lc.name NOT LIKE '%%해지%%'
                AND lc.name NOT LIKE '%%미사용%%'
                ;
                '''
        return query
        
    def query_brand(self):
        query ='select b.id, b.name from master."brands" b;'
        return query
        
    def query_channel(self):
        query ='select c.id, c.name from commerce.channels c;'
        return query
        
''' original
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
        c.name as "판매처명",
        b.name as "브랜드명",
        (CASE
            WHEN (i."deliverySheetId" IS NOT NULL) THEN 'Y'
            ELSE 'N'
        END) AS "B2B출고여부"
    FROM
        logistics."invoices" i
        LEFT JOIN master.partners p ON p.id = i."partnerId"
        LEFT JOIN logistics."logisticsCenters" lc ON lc.id = i."logisticsCenterId"
        LEFT JOIN master."brands" AS b ON b."id" = i."brandId"
        LEFT JOIN commerce.channels c ON c.id = i. "channelId"
    WHERE
        i."tempOrderSheetId" IS NULL
        AND i."isConfirmed" = TRUE
        AND i."isClosed" = TRUE
        AND i."isCanceled" = FALSE
        AND i."isPostponed" = FALSE
        AND i."isAccepted" = TRUE
        AND i."closeAt" AT TIME ZONE 'Asia/Seoul' >= '2022-01-01 04:00:00+09'::timestamp
        AND "i"."closeAt" AT TIME ZONE 'Asia/Seoul' < '2022-04-01 04:00:00+09'::timestamp
        AND lc.name NOT LIKE '%%해지%%'
        AND lc.name NOT LIKE '%%미사용%%'
    ORDER BY
        i."closeAt" ASC
'''