
		WITH offers_data AS (
			
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'4e1a8277-e63c-46a8-bc9b-e19c587c3f7f' AS offer_id,
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
				'999cab8c-5deb-433a-9a5d-fa42d34b1c42' AS offer_id,
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
				'7f2e7482-cb33-42b3-b5b5-63a8b26eb2c3' AS offer_id,
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
				'396db02f-e76d-445b-b0dd-dc232c4bf605' AS offer_id,
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
				'a50e4899-34df-43ea-9fa1-aff487ea1e07' AS offer_id,
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
				'43379096-79e2-401e-8cc8-124c020c5dac' AS offer_id,
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
				'ecbe7b8a-57ea-491a-92e0-dcb16c4c858d' AS offer_id,
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
				'8a71dc01-9301-48a0-a46c-55ad0d47e2a3' AS offer_id,
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
				'30899e15-1408-4b23-a2c6-d19d68bf8657' AS offer_id,
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
				'8951f50f-ee52-4515-8ab3-7611f36f78cc' AS offer_id,
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
				'baa0c9a6-4d11-4ecd-8c19-d48a5939cbfd' AS offer_id,
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
				'c99b651b-a02a-443f-b3be-9652c584fd25' AS offer_id,
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
				'1adf15f8-a3a3-4d4b-8fdf-c6ec69859bd5' AS offer_id,
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
				'bc4d4ced-c5fd-4184-8e1e-556359b6338f' AS offer_id,
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
				'52198080-37c7-43b1-a286-295c3262b067' AS offer_id,
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
				'9bfa9584-cdd3-47f6-be88-e4479b85b2bc' AS offer_id,
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
				'04700db1-eb10-45ec-9e37-6a2044e3d4f0' AS offer_id,
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
				'99dbbd92-a4e4-4bef-8446-01a050d2e122' AS offer_id,
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
				'c084cb48-f2df-4843-bb4a-ae36f4c9fabc' AS offer_id,
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
				'b6f32a01-45bf-4909-bd99-c4edcf9cd5af' AS offer_id,
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
				'9b8f3ccd-412e-4a60-8a03-4f781d797814' AS offer_id,
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
				'fc4d1079-8fcb-4d26-a9be-854b9a1d3152' AS offer_id,
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
				'ca31ee35-1614-47ed-8ac5-a2f5865c7e6d' AS offer_id,
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
				'21b2502e-0a4f-4a02-a342-3c1927a355ac' AS offer_id,
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
				'd07556e4-879b-4616-8da5-a9b480de3124' AS offer_id,
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
				'3438a960-1e44-4880-b049-88cf91865547' AS offer_id,
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
				'91ee6ece-3519-4495-958e-f1a8de9a6043' AS offer_id,
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
				'1a81ae79-3a8e-4752-ba1e-d59911f26df8' AS offer_id,
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
				'a4046a51-5a4a-4067-be3c-eda0908af51f' AS offer_id,
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
	