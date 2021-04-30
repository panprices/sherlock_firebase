
		WITH offers_data AS (
			
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'53a0aba5-5cfb-42de-9a4e-1c6e42850d3b' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_UK' AS offer_source,
				'Steiner Navy binoculars Navigator PRO C 7 x 50 mm Porro prism Dark blue 7155' AS retail_prod_name,
				'eBay' AS retailer_name,
				'UK' AS country,
				46900 AS price,
				'GBP' AS currency,
				'https://rover.ebay.com/rover/1/710-53481-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229508&item=221540161455' AS offer_url,
				'2020-11-20T09:50:37Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'3b75cd00-527b-4de9-ba5d-34fe9a4682c5' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Steiner Navigator Pro 7x50 Kompass Fernglas blau robust hohe Detailschärfe NEU' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				41990 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=202910552744' AS offer_url,
				'2020-11-20T09:50:37Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'559752ee-b7bd-47b1-bd16-35ab3ad5a1af' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Steiner Fernglas Navigator Pro 7x50 Kompass (7155)' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				53516 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=360930249730' AS offer_url,
				'2020-11-20T09:50:37Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'bfa429db-180f-44e7-9aa0-83223c5f0f4c' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Steiner Marine Fernglas Navigator Pro C 7 X 50 MM Porro Prisma Dunkelblau 7155' AS retail_prod_name,
				'eBay' AS retailer_name,
				'UK' AS country,
				56872 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=323152447382' AS offer_url,
				'2020-11-20T09:50:37Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'8d91b433-cfba-484e-bd7e-eaa17adfd1f3' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_FR' AS offer_source,
				'STEINER NAVIGATOR PRO 7X50C binocolo nautico e da escursionismo con bussola' AS retail_prod_name,
				'eBay' AS retailer_name,
				'IT' AS country,
				51000 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/709-53476-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229480&item=353148359439' AS offer_url,
				'2020-11-20T09:50:37Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'03ce27c7-3685-4277-aefc-284752862440' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_FR' AS offer_source,
				'Steiner Navy binoculars Navigator PRO C 7 x 50 mm Porro prism Dark blue 7155' AS retail_prod_name,
				'eBay' AS retailer_name,
				'UK' AS country,
				46900 AS price,
				'GBP' AS currency,
				'https://rover.ebay.com/rover/1/709-53476-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229480&item=221540161455' AS offer_url,
				'2020-11-20T09:50:37Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'f9f31552-f220-4b7e-ac16-8d9c245c2423' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_FR' AS offer_source,
				'Steiner Fernglas Navigator Pro 7x50 Kompass (7155)' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				53516 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/709-53476-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229480&item=360930249730' AS offer_url,
				'2020-11-20T09:50:37Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'f5a69959-ea67-465b-af10-09af3d1ce5b2' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_FR' AS offer_source,
				'Steiner Marine Jumelles Navigator PRO C 7 X 50 MM Prisme Porro Bleu Foncé 7155' AS retail_prod_name,
				'eBay' AS retailer_name,
				'UK' AS country,
				56251 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/709-53476-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229480&item=232702989653' AS offer_url,
				'2020-11-20T09:50:37Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'fc6b66b9-8094-460f-9b08-59beed420990' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_IT' AS offer_source,
				'STEINER NAVIGATOR PRO 7X50C binocolo nautico e da escursionismo con bussola' AS retail_prod_name,
				'eBay' AS retailer_name,
				'IT' AS country,
				51000 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/724-53478-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229494&item=353148359439' AS offer_url,
				'2020-11-20T09:50:37Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'73226666-1e79-4f4c-bec3-84e6f174c3bf' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_IT' AS offer_source,
				'Steiner Navy binoculars Navigator PRO C 7 x 50 mm Porro prism Dark blue 7155' AS retail_prod_name,
				'eBay' AS retailer_name,
				'UK' AS country,
				46900 AS price,
				'GBP' AS currency,
				'https://rover.ebay.com/rover/1/724-53478-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229494&item=221540161455' AS offer_url,
				'2020-11-20T09:50:37Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'e03181bc-f1c3-42d3-a6cc-bada445af8cd' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_IT' AS offer_source,
				'Steiner Fernglas Navigator Pro 7x50 Kompass (7155)' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				53516 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/724-53478-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229494&item=360930249730' AS offer_url,
				'2020-11-20T09:50:37Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'3a58a9e3-22c6-4736-95b1-0b5ef520ce01' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_IT' AS offer_source,
				'Steiner Blu Scuro Binocolo Navigator Pro C 7 X 50 MM Porro Prism 7155' AS retail_prod_name,
				'eBay' AS retailer_name,
				'UK' AS country,
				56872 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/724-53478-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229494&item=223379481742' AS offer_url,
				'2020-11-20T09:50:37Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				NULL AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				NULL AS source,
				NULL AS retailer_product_name,
				NULL AS retailer_name,
				NULL AS country,
				NULL AS price,
				NULL AS currency,
				NULL AS offer_url,
				NULL AS requested_at,
				NULL AS match_score
		), offers_raw AS ( ---- take retailer data, calculate the price and filter out blacklisted retailer
			SELECT
				A.*,
				C.domain,
				C.id AS retailer_id,
				C.offer_source_id,
				((A.price::int * E.to_sek) / 100)::int AS adj_price -- the price adjusted for the currency
			FROM offers_data A
			INNER JOIN offer_sources B
			ON A.offer_source = B.name
			FULL OUTER JOIN retailers C
			ON A.retailer_name = C.name AND B.id = C.offer_source_id
			INNER JOIN currency E
			ON A.currency = E.name
			WHERE COALESCE(C.blacklisted, FALSE) IS FALSE -- Remove blacklisted retailers
		-- Add shipping data (where we have it) to the raw offers rows
		), offers_with_shipping AS (
			SELECT
				DISTINCT ON (A.offer_id)
				A.*,
				B.ship,
		--		((B.min_order_val * C.to_sek) / 100)::int AS min_order_val, -- adjusted for the currency
		--		((B.fee * C.to_sek) / 100)::int AS fee, -- adjusted for the currency
				CASE
					-- When we don't have shipping data => return estimation in some cases
					WHEN fee IS NULL THEN
						CASE
							WHEN A.country IN ('UK', 'IT', 'ES') THEN 406
							WHEN A.country IN ('FR', 'BE', 'LU', 'DE') THEN 203
							WHEN A.country = 'SE' THEN 0
							ELSE NULL
						END
					-- When we have shipping and item price is higher then min order value => return the fee
					WHEN (((B.min_order_val * C.to_sek) / 100) > A.adj_price) THEN ((B.fee * C.to_sek) / 100)::int
					-- When we have shipping and there is no min order value => return the fee
					WHEN (min_order_val IS NULL AND fee IS NOT NULL) THEN ((B.fee * C.to_sek) / 100)::int
					ELSE NULL
				END AS shipping_fee
			FROM offers_raw A
			FULL OUTER JOIN shipping B
			ON A.retailer_id = B.retailer_id
			FULL OUTER JOIN currency C
			ON B.currency = C.name
		-- Add retailer trust data and determine quality score
		), offers_with_shipping_and_trust AS (
			SELECT
				DISTINCT ON (A.offer_id)
				A.*,
				(adj_price + shipping_fee) * 0.03 AS exchange_rate_fee,
				(adj_price + shipping_fee) * 0.05 AS service_fee,
				((adj_price + shipping_fee) * 0.05) * 0.25 AS vat,
				(adj_price + shipping_fee + ((adj_price + shipping_fee) * 0.05) + (((adj_price + shipping_fee) * 0.05) * 0.25)) * 0.014 AS payment_fee_se,
				(adj_price + shipping_fee + ((adj_price + shipping_fee) * 0.03) + ((adj_price + shipping_fee) * 0.05) + (((adj_price + shipping_fee) * 0.05) * 0.25)) * 0.014 AS payment_fee_int,
				CASE
					-- If no Trustpilot rating
					WHEN avg_rating = 0 THEN
						CASE
							WHEN site_rank < 15000 	THEN 5
							WHEN site_rank < 25000 	THEN 4.5
							WHEN site_rank < 50000 	THEN 4
							WHEN site_rank < 75000 	THEN 3
							WHEN site_rank < 100000 THEN 2
							WHEN site_rank < 300000 THEN 1.5
							WHEN site_rank < 500000 THEN 1
						END
					-- If we have Truspilot rating, start with that rating and then credit or discredit it
					WHEN avg_rating != 0 THEN (
						SELECT (
							avg_rating +
							CASE
								WHEN num_ratings < 50 THEN -1
								WHEN num_ratings < 100 THEN -0.5
								WHEN num_ratings > 1000 THEN 0.5
								WHEN num_ratings > 5000 THEN 1
								ELSE 0
							END +
							CASE WHEN num_ratings < 100 THEN -0.5 ELSE 0 END +
							CASE
								WHEN site_rank < 15000 THEN 1
								WHEN site_rank < 25000 THEN 0.5
								WHEN site_rank > 100000 THEN -0.5
								WHEN (site_rank > 300000) OR (site_rank IS NULL) THEN -1
								ELSE 0
							END
						)
					)
				END AS quality_score
			FROM offers_with_shipping A
			FULL OUTER JOIN alexa B
			ON A.domain = B.retailer_domain
			FULL OUTER JOIN trustpilot C
			ON A.domain = C.retailer_domain
		), lowest_local_price AS ( ---- get the lowest price among offers, this step touch all the offers including old and new
			SELECT
				MIN(adj_price)
			FROM offers_with_shipping_and_trust
			WHERE country = 'SE' -- Change this when we launch another country
		/*
			Filter out offers:
				1. That do not pass the string matching test.
				2. Which are from black listed retailers (second hand for example)
				3. Which we have duplicates on, for example the same from prisjakt as pricerunner
		*/
		), offers_filtered AS (
			SELECT
				-- Filter out when we from different sources have gotten the same offer
				DISTINCT ON (A.retail_prod_name, A.domain, A.adj_price)
				A.*
			FROM offers_with_shipping_and_trust A
			LEFT JOIN (
				SELECT
					A.*
				FROM offers_with_shipping_and_trust A
				-- Grab the offer sources to filter dynamically to be able to configure
				-- this on the fly without having to re-deploy stuff
				WHERE A.offer_source SIMILAR TO (
					SELECT
						array_to_string(
								array(
										SELECT
											name
										FROM offer_sources
										WHERE filter IS TRUE
								), '|'
						)
				)
				AND (
								(A.adj_price < (SELECT * FROM lowest_local_price) *  (SELECT value FROM offer_filters WHERE name = 'min_price')) OR
								(A.adj_price > (SELECT * FROM lowest_local_price) *  (SELECT value FROM offer_filters WHERE name = 'max_price'))
						)
			) C
			ON A.offer_id = C.offer_id
			WHERE C.offer_id IS NULL
			-- Blacklisted retailers
			AND lower(A.retailer_name) NOT SIMILAR TO 'bluecity%|datapryl%'
		), offers_complete AS (
			SELECT
				*,
				--((SELECT * FROM lowest_local_price) - B.adj_price) AS saving,
				CASE
					WHEN shipping_fee IS NOT NULL AND offer_source != 'google_shopping_SE' AND country != 'SE' THEN true
					ELSE false
				END AS direct_checkout,
				CASE
					WHEN shipping_fee IS NOT NULL AND offer_source != 'google_shopping_SE' AND country != 'SE' THEN
						CASE
							WHEN country != 'SE' THEN ((adj_price + shipping_fee + service_fee + vat + payment_fee_int + exchange_rate_fee) )
							ELSE NULL
						END
					ELSE NULL
				END AS direct_checkout_price,
				CASE
					WHEN quality_score > 5 THEN 5
					WHEN quality_score < 1 THEN 1
					ELSE quality_score::float
				END AS quality_score_adjusted
			FROM (
				SELECT
					*
				FROM offers_filtered
			) B
		)
		SELECT
			updated_at,
			product_id,
			offer_source,
			retail_prod_name,
			retailer_name,
			country,
			(adj_price * 100)::int AS price,
			'SEK' AS currency,
			offer_url,
			quality_score_adjusted AS quality_score,
			domain,
			CASE -- Ship should be true when direct_checkout is enabled
				WHEN direct_checkout IS TRUE THEN TRUE
				ELSE ship
			END AS ship,
			(shipping_fee * 100)::int AS shipping_fee,
			(((SELECT * FROM lowest_local_price) - adj_price) * 100)::int AS saving,
			CASE -- Only show saving when direct_checkout is enabled
				WHEN direct_checkout IS TRUE THEN
					CASE -- Only show saving when the offer is less expensive then the Swedish one
				 		WHEN ((SELECT * FROM lowest_local_price) > direct_checkout_price) THEN (((SELECT * FROM lowest_local_price) - direct_checkout_price) * 100)::int
						ELSE NULL
					END
				ELSE NULL
			END AS direct_checkout_saving,
			offer_id,
			direct_checkout,
			(direct_checkout_price * 100)::int AS direct_checkout_price,
			CASE -- Don't enable concierge on Swedish offers or when direct_checkout is enabled
				WHEN direct_checkout IS FALSE AND country != 'SE' THEN TRUE
				ELSE FALSE
			END AS concierge
		FROM offers_complete
		WHERE offer_source IS NOT NULL-- Remove the row needed for the union
		AND offer_source NOT LIKE 'google_shopping%'-- TEMPORARY REMOVE GOOGLE SHOPPING
		ORDER BY direct_checkout_price ASC;
	