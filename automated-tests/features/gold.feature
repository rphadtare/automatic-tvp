Feature: Transformation to gold layer

  Scenario: Process data from silver layer
    Given there is silver data at path '/silver/eth_transfers_data_quarantined/year=2024/month=11/*.parquet':
      |block_date|tx_hash	                                                        |token_category| event_name  |amount_usd |vertical	                                |protocol                               |week_of_month|week_of_year|
      |2024-11-23|0xbcd356bedca492c633d68ec9811eb8a95b19746fdea397ea36798dca0b997616|stablecoin	   | Transfer	 |100.00     |unclassified transfer to EOA	            |unclassified transfer to EOA           |4            |47          |
      |2024-11-23|0x2a5cc879685777e9c3b20bf6a890c6e5de6df1be02c4b39019592ff02e45c540|stablecoin	   | Transfer	 |150.00     |unclassified transfer to Safe	            |Safe                                   |4            |47          |
      |2024-11-23|0x45e5147bb5c3830678d1be205ddaa4a1753fcb75336a6295f404d67e28fef7cf|erc20	       | Transfer	 |15500.00   |unclassified transfer to Safe	            |Safe                                   |4            |47          |
      |2024-11-23|0xf1f08c7a3022885706761312ac67080a5e124125069169ec33973e922fe1ea83|stablecoin	   | Transfer	 |3600.500   |unclassified transfer to EOA	            |unclassified transfer to EOA           |4            |47          |
      |2024-11-23|0x1b89112706eb08628ef57059486dad3cb056b4dcdeb04fba2f74373f80c1aec5|stablecoin	   | Transfer	 |750.5000   |unclassified transfer to EOA	            |unclassified transfer to EOA           |4            |47          |
      |2024-11-23|0x5845b39d98f03e1277353ec7eb273597c840377867ff548140a538167fc93a82|erc20	       | Transfer	 |5000.00    |unclassified transfer to EOA	            |unclassified transfer to EOA           |4            |47          |
      |2024-11-23|0xb535433c2b9aa7cc0d7437b74793f24c3028cc4d44ed80cd7ca1bac1b17646a7|erc20	       | Transfer	 |40000	     |unclassified transfer to EOA	            |unclassified transfer to EOA           |4            |47          |
      |2024-11-23|0x47e7003df915fb04e73ff78f9c90d1def74094ea69656a6e37283501add8ef9b|stablecoin	   | Transfer	 |100.500    |unclassified transfer to EOA	            |unclassified transfer to EOA           |4            |47          |
      |2024-11-23|0x277726002ed82fd04d4fa0d347f11a2e6e07652bfb1bd1adfeb234f4672d81c0|erc20	       | Transfer	 |100.500    |unclassified transfer to EOA	            |unclassified transfer to EOA           |4            |47          |
      |2024-11-23|0xf000a9326e47f5b2fa12c6648d1f4e81855077cd3db43bc384dbd1f726f679ec|erc20	       | Transfer	 |450.500    |unclassified transfer to EOA	            |unclassified transfer to EOA           |4            |47          |
      |2024-11-23|0x7e336c27aca333968c242e7bb21e165a0b69e005113421eb42badbc952849b7b|erc20	       | Transfer	 |0.750      |unclassified transfer to Smart contract	|unclassified transfer to Smart contract|4            |47          |
      |2024-11-23|0x7e336c27aca333968c242e7bb21e165a0b69e005113421eb42badbc952849b7b|erc20	       | Transfer	 |0.250      |unclassified transfer to Smart contract	|unclassified transfer to Smart contract|4            |47          |
      |2024-11-23|0x7e336c27aca333968c242e7bb21e165a0b69e005113421eb42badbc952849b7b|erc20	       | Transfer	 |1.000      |unclassified transfer to Smart contract	|unclassified transfer to Smart contract|4            |47          |
    And There is no data present in existing gold layer at path '/gold/eth_transfers_data_aggregated_vertical/year=2024/month=11/' and '/gold/eth_transfers_data_aggregated_protocol/year=2024/month=11/'

    When transforming data into gold layer

    Then final vertical gold layer at path '/gold/eth_transfers_data_aggregated_vertical/year=2024/month=11/*.parquet':
      |vertical	                               |week_of_month|week_of_year|safe_account	                                                        |total_amount_usd_per_safe |unique_safes|total_transactions_per_safe|
      |unclassified transfer to EOA	           |4            |47          |0xbcd356bedca492c633d68ec9811eb8a95b19746fdea397ea36798dca0b997616   |100.00                    |8           |1                          |
      |unclassified transfer to EOA	           |4            |47          |0xf1f08c7a3022885706761312ac67080a5e124125069169ec33973e922fe1ea83   |3600.500                  |8           |1                          |
      |unclassified transfer to EOA	           |4            |47          |0x1b89112706eb08628ef57059486dad3cb056b4dcdeb04fba2f74373f80c1aec5   |750.5000                  |8           |1                          |
      |unclassified transfer to EOA	           |4            |47          |0x5845b39d98f03e1277353ec7eb273597c840377867ff548140a538167fc93a82   |5000.00                   |8           |1                          |
      |unclassified transfer to EOA	           |4            |47          |0xb535433c2b9aa7cc0d7437b74793f24c3028cc4d44ed80cd7ca1bac1b17646a7   |40000	                   |8           |1                          |
      |unclassified transfer to EOA	           |4            |47          |0x47e7003df915fb04e73ff78f9c90d1def74094ea69656a6e37283501add8ef9b   |100.500                   |8           |1                          |
      |unclassified transfer to EOA	           |4            |47          |0x277726002ed82fd04d4fa0d347f11a2e6e07652bfb1bd1adfeb234f4672d81c0   |100.500                   |8           |1                          |
      |unclassified transfer to EOA	           |4            |47          |0xf000a9326e47f5b2fa12c6648d1f4e81855077cd3db43bc384dbd1f726f679ec   |450.500                   |8           |1                          |
      |unclassified transfer to Smart contract |4            |47          |0x7e336c27aca333968c242e7bb21e165a0b69e005113421eb42badbc952849b7b   |2.00                      |1           |3                          |
      |unclassified transfer to Safe	       |4            |47          |0x2a5cc879685777e9c3b20bf6a890c6e5de6df1be02c4b39019592ff02e45c540   |150.00                    |2           |1                          |
      |unclassified transfer to Safe	       |4            |47          |0x45e5147bb5c3830678d1be205ddaa4a1753fcb75336a6295f404d67e28fef7cf   |15500.00                  |2           |1                          |

    And final protocol gold layer at path '/gold/eth_transfers_data_aggregated_protocol/year=2024/month=11/*.parquet':
      |protocol	                               |week_of_month|week_of_year|safe_account	                                                        |total_amount_usd_per_safe |unique_safes|total_transactions_per_safe|
      |unclassified transfer to EOA	           |4            |47          |0xbcd356bedca492c633d68ec9811eb8a95b19746fdea397ea36798dca0b997616   |100.00                    |8           |1                          |
      |unclassified transfer to EOA	           |4            |47          |0xf1f08c7a3022885706761312ac67080a5e124125069169ec33973e922fe1ea83   |3600.500                  |8           |1                          |
      |unclassified transfer to EOA	           |4            |47          |0x1b89112706eb08628ef57059486dad3cb056b4dcdeb04fba2f74373f80c1aec5   |750.5000                  |8           |1                          |
      |unclassified transfer to EOA	           |4            |47          |0x5845b39d98f03e1277353ec7eb273597c840377867ff548140a538167fc93a82   |5000.00                   |8           |1                          |
      |unclassified transfer to EOA	           |4            |47          |0xb535433c2b9aa7cc0d7437b74793f24c3028cc4d44ed80cd7ca1bac1b17646a7   |40000	                   |8           |1                          |
      |unclassified transfer to EOA	           |4            |47          |0x47e7003df915fb04e73ff78f9c90d1def74094ea69656a6e37283501add8ef9b   |100.500                   |8           |1                          |
      |unclassified transfer to EOA	           |4            |47          |0x277726002ed82fd04d4fa0d347f11a2e6e07652bfb1bd1adfeb234f4672d81c0   |100.500                   |8           |1                          |
      |unclassified transfer to EOA	           |4            |47          |0xf000a9326e47f5b2fa12c6648d1f4e81855077cd3db43bc384dbd1f726f679ec   |450.500                   |8           |1                          |
      |unclassified transfer to Smart contract |4            |47          |0x7e336c27aca333968c242e7bb21e165a0b69e005113421eb42badbc952849b7b   |2.00                      |1           |3                          |
      |Safe	                                   |4            |47          |0x2a5cc879685777e9c3b20bf6a890c6e5de6df1be02c4b39019592ff02e45c540   |150.00                    |2           |1                          |
      |Safe	                                   |4            |47          |0x45e5147bb5c3830678d1be205ddaa4a1753fcb75336a6295f404d67e28fef7cf   |15500.00                  |2           |1                          |