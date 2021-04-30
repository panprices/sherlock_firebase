
		WITH offers_data AS (
			
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'd27b265d-6b1d-4c2a-939c-95f1c2fe98a2' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_UK' AS offer_source,
				'BRAND NEW Samsung Galaxy S20+ UNLOCKED 8GB RAM +128GB ROM' AS retail_prod_name,
				'eBay' AS retailer_name,
				'UK' AS country,
				64949.0 AS price,
				'GBP' AS currency,
				'https://rover.ebay.com/rover/1/710-53481-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229508&item=264851653285' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'07834fdc-b356-4195-9ced-24f5e2682957' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_UK' AS offer_source,
				'Samsung Galaxy S20+ Plus (4G) 128GB SM-G985F/DS Dual-SIM Grey Unlocked SIMFree' AS retail_prod_name,
				'eBay' AS retailer_name,
				'UK' AS country,
				111900.0 AS price,
				'GBP' AS currency,
				'https://rover.ebay.com/rover/1/710-53481-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229508&item=293508513079' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'7b3b3a8a-6159-450e-a774-46ae74c4a4bc' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Samsung Galaxy S20+ SM-G985F/DS - 128GB - Cosmic Gray (Ohne Simlock) (Dual SIM)' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				66900.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=353308788630' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'81927c6b-791c-4fb7-9396-c507e2ab9c8f' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Samsung G985F Galaxy  S20+ 128 GB (Cosmic Gray), 64 MPixel Kamera, NEU OVP' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				81240.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=184337778393' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'729efff5-aa61-480b-89bb-79a881950ec6' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'SAMSUNG GALAXY S20+ PLUS 128GB COSMIC GRAY G985F/DS ANDROID LTE SM-G985FZADEUB' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				69999.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=353301191843' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'859723aa-28cb-41d8-afd9-ca3b7025c90c' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Samsung G985 galaxy S20+ LTE 8GB RAM 128GB dual kosmisches grau' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				76380.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=284074842877' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'129acef0-9a7d-4bf4-9b5d-3f6465be634c' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Samsung Galaxy S20+ 128GB Cosmic Gray, Smartphone (DUAL-SIM, Android 10)' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				80581.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=143658748496' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'330394f2-dc0d-4786-a4d8-bcb9ad069b18' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Samsung G985F Galaxy  S20+ 128 GB (Cosmic Gray), 64 MPixel Kamera, NEU OVP' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				81985.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=293604547341' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'192c13ed-058f-48e0-bc82-2bf02f49a649' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Samsung G985F Galaxy  S20+ 128 GB (Cosmic Gray), 64 MPixel Kamera, NEU OVP' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				81895.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=174260409587' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'6e704f4e-9671-4f11-820d-4827c7552e6f' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Samsung G985F Galaxy  S20+ 128 GB (Cosmic Gray), 64 MPixel Kamera, NEU OVP' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				81228.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=264784192992' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'e2627f12-af3f-4c71-b565-92a2e59953ea' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Samsung Galaxy S20+ G985F 128GB Cosmic Grey 8GB RAM 4G LTE Bluetooth Android' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				77090.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=143564256525' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'f5fa4796-b9d0-4a8f-8243-a9e807b4580b' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Samsung Galaxy S20 Plus 128GB 6,7 Zoll Smartphone QHD+ Triple Kamera Handy grau' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				81299.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=264679288338' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'4dc60631-4894-41cb-a4dd-ba7e99b2d88f' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Samsung G985F Galaxy  S20+ 128 GB (Cosmic Gray), 64 MPixel Kamera, NEU OVP' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				81695.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=254624639694' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'a72851b0-00f2-40cb-bdf1-a4d207a51fa3' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Samsung G985F Galaxy  S20+ 128 GB (Cosmic Gray), 64 MPixel Kamera, NEU OVP' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				82729.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=184420567442' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'8f288af9-815b-44d7-a966-78f9ea79e46d' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Samsung G985F Galaxy  S20+ 128 GB (Cosmic Gray), 64 MPixel Kamera, NEU OVP' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				89990.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=283886879353' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'20dd3968-2baf-4bff-a84c-3c78fa9d989a' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'> Samsung Galaxy S20+ Smartphone [Cosmic Gray|128GB|4G|Dual-SIM|Android 10]' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				83390.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=362955151300' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'1dafdae9-94cf-4eae-8d40-5e9a66f1f613' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'Samsung Galaxy S20+ Cosmic Gray 128GB -' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				74821.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=264963737342' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'9eb6be44-03bd-40fd-aaf7-b0e61e25d7b5' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_DE' AS offer_source,
				'BRAND NEW Samsung Galaxy S20+ UNLOCKED 8GB RAM +128GB ROM' AS retail_prod_name,
				'eBay' AS retailer_name,
				'UK' AS country,
				64949.0 AS price,
				'GBP' AS currency,
				'https://rover.ebay.com/rover/1/707-53477-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229487&item=264851653285' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'451ec4b8-425b-4c22-9d88-cd9529f7c55f' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_FR' AS offer_source,
				'Smartphone Samsung Galaxy S20+ Plus Cosmic Gray 128gb' AS retail_prod_name,
				'eBay' AS retailer_name,
				'ES' AS country,
				64595.00000000001 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/709-53476-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229480&item=164458184158' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'432e4557-6e39-4d86-83bb-0994e5d61542' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_FR' AS offer_source,
				'Samsung Galaxy S20+ 128 Go Gris' AS retail_prod_name,
				'eBay' AS retailer_name,
				'FR' AS country,
				85708.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/709-53476-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229480&item=363132539323' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'58c7905f-21a5-4e3e-ae46-a902256a2b7a' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_FR' AS offer_source,
				'BRAND NEW Samsung Galaxy S20+ UNLOCKED 8GB RAM +128GB ROM' AS retail_prod_name,
				'eBay' AS retailer_name,
				'UK' AS country,
				64949.0 AS price,
				'GBP' AS currency,
				'https://rover.ebay.com/rover/1/709-53476-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229480&item=264851653285' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'4ca599fc-c7d4-4081-b4bb-3593e8c32894' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_FR' AS offer_source,
				'Samsung Galaxy S20+ Plus (4G) 128GB SM-G985F/DS Dual-SIM Grey Unlocked SIMFree' AS retail_prod_name,
				'eBay' AS retailer_name,
				'UK' AS country,
				111900.0 AS price,
				'GBP' AS currency,
				'https://rover.ebay.com/rover/1/709-53476-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229480&item=293508513079' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'847a9b13-78af-479b-90fd-1cfc2aa497cf' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_FR' AS offer_source,
				'Smartphone Samsung Galaxy S20+ Plus Cosmic Gray 128gb' AS retail_prod_name,
				'eBay' AS retailer_name,
				'ES' AS country,
				70325.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/709-53476-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229480&item=164533339267' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'914929fb-0e55-42c2-b212-0a7ea7319e01' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_FR' AS offer_source,
				'> Samsung Galaxy S20+ Smartphone [Cosmic Gray|128GB|4G|Dual-SIM|Android 10]' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				83390.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/709-53476-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229480&item=362955151300' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'7436ee43-46a6-4453-8947-987f9120f56f' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_IT' AS offer_source,
				'Smartphone Samsung Galaxy S20+ Plus Cosmic Gray 128gb' AS retail_prod_name,
				'eBay' AS retailer_name,
				'ES' AS country,
				64595.00000000001 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/724-53478-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229494&item=164458184158' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'ea635b67-91e9-4417-8b05-8b06617d1e19' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_IT' AS offer_source,
				'Samsung Galaxy S20+ SM-G985F/DS - 128GB - Cosmic Gray (Ohne Simlock) (Dual SIM)' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				66900.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/724-53478-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229494&item=353308788630' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'93241d26-4ea4-4c80-b4a3-1d5e54d3c534' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_IT' AS offer_source,
				'BRAND NEW Samsung Galaxy S20+ UNLOCKED 8GB RAM +128GB ROM' AS retail_prod_name,
				'eBay' AS retailer_name,
				'UK' AS country,
				64949.0 AS price,
				'GBP' AS currency,
				'https://rover.ebay.com/rover/1/724-53478-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229494&item=264851653285' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'61c09cec-948b-4c83-aa71-42ff3ab5af19' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_IT' AS offer_source,
				'Samsung Galaxy S20+ Plus (4G) 128GB SM-G985F/DS Dual-SIM Grey Unlocked SIMFree' AS retail_prod_name,
				'eBay' AS retailer_name,
				'UK' AS country,
				111900.0 AS price,
				'GBP' AS currency,
				'https://rover.ebay.com/rover/1/724-53478-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229494&item=293508513079' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'495211b1-059c-4868-80b9-e9f73afbbc85' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'ebay_IT' AS offer_source,
				'> Samsung Galaxy S20+ Smartphone [Cosmic Gray|128GB|4G|Dual-SIM|Android 10]' AS retail_prod_name,
				'eBay' AS retailer_name,
				'DE' AS country,
				83390.0 AS price,
				'EUR' AS currency,
				'https://rover.ebay.com/rover/1/724-53478-19255-0/1?ff3=2&toolid=10044&campid=5338271690&customid=&lgeo=1&vectorid=229494&item=362955151300' AS offer_url,
				'2020-12-08T15:45:23Z' AS requested_at,
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
	