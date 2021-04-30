
		WITH offers_data AS (
			
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'5e612f55-f4c3-403b-85c9-18a3f9200a2f' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_DE' AS offer_source,
				'Samsung Galaxy S20+   8 GB   128 GB   cosmic gray' AS retail_prod_name,
				'Refurbed GmbH' AS retailer_name,
				'DE' AS country,
				'69200' AS price,
				'EUR' AS currency,
				'https://de-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348226&.sig=Bw86NQ8F6A5fOZ6E1WHwpV5ODfo-&affiliationId=96954745&catId=100020213&comId=100512653&contextLevel=1&contextOfferPosition=1&contextPageSize=9&country=de&ecs=ok&merchantid=100512653&offerId=2cb0395d45a989fb58b1f428ae0c452b&searchId=1076992015568_1607443348219_204587&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.276035' AS requested_at,
				1::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'cce25679-505f-491b-951f-ef996eea1eba' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_DE' AS offer_source,
				'Samsung Galaxy S20+ Smartphone (16,95 cm/6,7 Zoll, 128 GB Speicherplatz, 12 MP Kamera), Cosmic Gray' AS retail_prod_name,
				'Otto' AS retailer_name,
				'DE' AS country,
				'76900' AS price,
				'EUR' AS currency,
				'https://de-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348226&.sig=1Sf_tI3Lkch_6UaaJIGGL71gDC4-&affiliationId=96954745&catId=100020213&comId=3454923&contextLevel=1&contextOfferPosition=2&contextPageSize=9&country=de&ecs=ok&merchantid=3454923&offerId=9b5cb6190f793c6e1ca4759b5614e022&searchId=1076992015568_1607443348219_204587&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.276063' AS requested_at,
				1::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'4d6520af-1737-4439-b017-2eabc2d1ca21' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_DE' AS offer_source,
				'Samsung Gebraucht: Samsung Galaxy S20+ 4G G985F/DS 128GB grau' AS retail_prod_name,
				'asgoodasnew.de' AS retailer_name,
				'DE' AS country,
				'58300' AS price,
				'EUR' AS currency,
				'https://de-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348227&.sig=ExieQqRAc3f2QTVyjqgbyyNw6cA-&affiliationId=96954745&catId=100020213&comId=100474979&contextLevel=1&contextOfferPosition=3&contextPageSize=9&country=de&ecs=ok&merchantid=100474979&offerId=c29be6adc097a2c990d243de9357dd94&searchId=1076992015568_1607443348219_204587&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.276069' AS requested_at,
				1::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'c26f9250-7ff2-408d-91b7-b06dfdbdc3ba' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_DE' AS offer_source,
				'Samsung Galaxy S20+ Dual-SIM-Smartphone cosmic gray 128 GB' AS retail_prod_name,
				'bueroshop24' AS retailer_name,
				'DE' AS country,
				'97000' AS price,
				'EUR' AS currency,
				'https://de-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348227&.sig=osEB4FUO1ircCOZv7ETycp8eV7Y-&affiliationId=96954745&catId=100020213&comId=100454454&contextLevel=1&contextOfferPosition=4&contextPageSize=9&country=de&ecs=ok&merchantid=100454454&offerId=4a22a3f9b91f4640fdadbee4bb175c15&searchId=1076992015568_1607443348219_204587&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.276073' AS requested_at,
				1::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'0f168aba-1e43-4fd4-86c9-7a1255259b7e' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_DE' AS offer_source,
				'Samsung Galaxy S20+ 4G Smartphone 17 cm (6.7 Zoll) 128 GB 2,73 GHz Android 12 MP Vierfach (Grau)' AS retail_prod_name,
				'Boomstore' AS retailer_name,
				'DE' AS country,
				'93300' AS price,
				'EUR' AS currency,
				'https://de-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348227&.sig=46t520i99Bfr2vtR5FNohjmIbzM-&affiliationId=96954745&catId=100020213&comId=100510929&contextLevel=1&contextOfferPosition=5&contextPageSize=9&country=de&ecs=ok&merchantid=100510929&offerId=0f09f79f121dced84ac041ec3c416740&searchId=1076992015568_1607443348219_204587&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.276078' AS requested_at,
				1::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'da302a63-7291-4081-877b-c51d3a9a41ad' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_DE' AS offer_source,
				'Samsung SAMS GALS20+GR - Samsung Galaxy S20+ 128 GB Cosmic Grey' AS retail_prod_name,
				'reichelt.de' AS retailer_name,
				'DE' AS country,
				'76900' AS price,
				'EUR' AS currency,
				'https://de-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348227&.sig=YoUoCmUTb_hsLw1ncjuU0vsQlPU-&affiliationId=96954745&catId=100020213&comId=100510912&contextLevel=1&contextOfferPosition=6&contextPageSize=9&country=de&ecs=ok&merchantid=100510912&offerId=ac806fcf2252d482a50a5a693e2145e7&searchId=1076992015568_1607443348219_204587&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.276082' AS requested_at,
				1::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'30a5a812-b5df-4ca0-9249-6b23f46517f7' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_DE' AS offer_source,
				'Samsung Wie neu: Samsung Galaxy S20+   8 GB   128 GB   cosmic gray' AS retail_prod_name,
				'Refurbed GmbH' AS retailer_name,
				'DE' AS country,
				'79900' AS price,
				'EUR' AS currency,
				'https://de-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348227&.sig=QvwFYfwEveih8u2eba549SKTDWo-&affiliationId=96954745&catId=100020213&comId=100512653&contextLevel=1&contextOfferPosition=7&contextPageSize=9&country=de&ecs=ok&merchantid=100512653&offerId=7a23ab3008cb780dc4269677620f8eb2&searchId=1076992015568_1607443348219_204587&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.276086' AS requested_at,
				1::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'd9cc74a6-d8c7-4a3f-99e5-d3114f1b06fa' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_DE' AS offer_source,
				'Samsung Gebraucht: Samsung Galaxy S20+ 4G G985F/DS 128GB grau' AS retail_prod_name,
				'asgoodasnew.de' AS retailer_name,
				'DE' AS country,
				'66300' AS price,
				'EUR' AS currency,
				'https://de-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348227&.sig=3S70oF0O9xe8VmTz7WuklbXv270-&affiliationId=96954745&catId=100020213&comId=100474979&contextLevel=1&contextOfferPosition=8&contextPageSize=9&country=de&ecs=ok&merchantid=100474979&offerId=9b0374ae8074b883801a91b27834c425&searchId=1076992015568_1607443348219_204587&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.276090' AS requested_at,
				1::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'f7bc792e-a551-4ddd-ad9c-6a380999f83e' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_DE' AS offer_source,
				'Samsung Galaxy S20+ 4G G985F/DS 128GB grau' AS retail_prod_name,
				'asgoodasnew.de' AS retailer_name,
				'DE' AS country,
				'68300' AS price,
				'EUR' AS currency,
				'https://de-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348227&.sig=gzMTHJoxN2wgvdjmDQjRK_7BJLA-&affiliationId=96954745&catId=100020213&comId=100474979&contextLevel=1&contextOfferPosition=9&contextPageSize=9&country=de&ecs=ok&merchantid=100474979&offerId=4a35c9617975a6ec75160c917f499e23&searchId=1076992015568_1607443348219_204587&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.276095' AS requested_at,
				1::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'ba2fc0bf-89de-4606-af11-4a240a7b8754' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_FR' AS offer_source,
				'Samsung Smartphone SAMSUNG GALAXY S20 Plus Gris 128Go' AS retail_prod_name,
				'Ubaldi' AS retailer_name,
				'FR' AS country,
				'100900' AS price,
				'EUR' AS currency,
				'https://fr-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348321&.sig=p.VpwJK4IxvN4RvuVbG3x9wUUsM-&affiliationId=96954746&catId=100020213&comId=100478128&contextLevel=1&contextOfferPosition=1&contextPageSize=4&country=fr&ecs=ok&merchantid=100478128&offerId=1611af3cdb8a2b25a1a76023bb6f7351&searchId=10769920723309_1607443348317_1086875&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.340573' AS requested_at,
				1::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'58972263-c9bd-4e9d-b418-907b0ed9fe87' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_FR' AS offer_source,
				'Samsung Galaxy S20+ 4G G985F/DS 128Go gris reconditionné' AS retail_prod_name,
				'Asgoodasnew.fr' AS retailer_name,
				'FR' AS country,
				'56900' AS price,
				'EUR' AS currency,
				'https://fr-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348321&.sig=dlBTjl3L8i1xKZ5O1XmJzMhblg8-&affiliationId=96954746&catId=100020213&comId=100508043&contextLevel=1&contextOfferPosition=2&contextPageSize=4&country=fr&ecs=ok&merchantid=100508043&offerId=c6bd1f844fe6868824cb337bb7cb55a8&searchId=10769920723309_1607443348317_1086875&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.340599' AS requested_at,
				1::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'd1bbe25c-9654-402f-bc51-8fe0f41284e5' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_FR' AS offer_source,
				'Samsung Galaxy S20+ 4G G985F/DS 128Go gris reconditionné' AS retail_prod_name,
				'Asgoodasnew.fr' AS retailer_name,
				'FR' AS country,
				'64900' AS price,
				'EUR' AS currency,
				'https://fr-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348322&.sig=JvxZKvHQjrWgC.rzFc0QqZCV9I8-&affiliationId=96954746&catId=100020213&comId=100508043&contextLevel=1&contextOfferPosition=3&contextPageSize=4&country=fr&ecs=ok&merchantid=100508043&offerId=fa4b74ac24fc0a10c3509dde01cf5dc8&searchId=10769920723309_1607443348317_1086875&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.340604' AS requested_at,
				1::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'b536e04c-9c1e-4501-8d8d-31cfb2f7e740' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_FR' AS offer_source,
				'Samsung Galaxy S20+ 4G G985F/DS 128Go gris new' AS retail_prod_name,
				'Asgoodasnew.fr' AS retailer_name,
				'FR' AS country,
				'66900' AS price,
				'EUR' AS currency,
				'https://fr-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348322&.sig=4cvgsecgv0fLA4qSefqEgvLrPrI-&affiliationId=96954746&catId=100020213&comId=100508043&contextLevel=1&contextOfferPosition=4&contextPageSize=4&country=fr&ecs=ok&merchantid=100508043&offerId=38ccfe38b0f2e53f794bad43202083b8&searchId=10769920723309_1607443348317_1086875&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.340609' AS requested_at,
				1::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'0fea0506-3629-448b-abe1-b5441112a4be' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_UK' AS offer_source,
				'Galaxy S20+ 128 GB   Cosmic Grey Unlocked (Refurbished)' AS retail_prod_name,
				'Backmarket UK' AS retailer_name,
				'UK' AS country,
				'69800' AS price,
				'GBP' AS currency,
				'https://uk-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348629&.sig=1G4y1iSSY4wJPfO4XeclX1lLZKg-&affiliationId=96954752&catId=100020213&comId=100508457&contextLevel=1&contextOfferPosition=1&contextPageSize=1&country=uk&ecs=ok&merchantid=100508457&offerId=38ce4d0a72c633fd2ad1394d21ab6cd9&searchId=1076981991630_1607443348626_131429&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.637400' AS requested_at,
				1::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'f94083db-8199-435c-9d3e-ba030af34fcd' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'kelkoo_NL' AS offer_source,
				'Samsung Galaxy S20+ Smartphone (16,95 cm / 6,7 inch, 128 GB, 12 MP Camera)  - 999.00 - grijs' AS retail_prod_name,
				'Otto.nl' AS retailer_name,
				'NL' AS country,
				'99900' AS price,
				'EUR' AS currency,
				'https://nl-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1607443348685&.sig=4M4s5G2REPIpRLh1z_bL6nPd0Bc-&affiliationId=96954751&catId=100020213&comId=100472987&contextLevel=1&contextOfferPosition=1&contextPageSize=1&country=nl&ecs=ok&merchantid=100472987&offerId=faef7ee40ce12839276b28a78fe11619&searchId=10769920127600_1607443348682_45674&searchQuery=&service=5&wait=true' AS offer_url,
				'2020-12-08 16:02:28.692815' AS requested_at,
				1::int AS match_score
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
	