with
    safe_transfers AS (
        select
            *
        from
            tokens_ethereum.transfers
        where
            block_date = date_add('day', -1, current_date)
            AND "from" IN (
                SELECT
                    address
                from
                    safe_ethereum.safes
            )
            AND amount_usd > 0
    ),
    receiver_labels AS (
        select
            t.blockchain,
            t.to AS address,
            MIN(la.name) AS label_names, --shouldn't have duplicates, but putting min() just in case so we don't double count
            MIN(la.category) AS label_categories,
            MIN(la.model_name) AS label_models,
            MIN(lc.name) AS contract_name,
            MIN(ens.name) AS ens
        from
            safe_transfers t
            LEFT JOIN ens.resolver_latest ens ON t.to = ens.address
            LEFT JOIN labels.contracts lc ON t.to = lc.address
            AND t.blockchain = lc.blockchain
            LEFT JOIN labels.addresses la ON t.to = la.address
            AND t.blockchain = la.blockchain
            AND la.category NOT IN ('contracts') --only useful to get the contract name, which we are getting from the join above
            AND la.category NOT IN ('social') --just flags if the contract has ens name, not helpful
            AND la.model_name NOT IN ('validators_ethereum')
            AND (
                la.label_type NOT IN ('persona', 'usage')
                OR la.model_name IN ('dex_pools', 'dao_framework', 'mev', 'flashbots')
            )
        group by
            1,
            2
    ),
    evt_names AS (
        SELECT DISTINCT
            tx_hash,
            index,
            event_name
        FROM
            ethereum.logs_decoded
        WHERE
            tx_hash IN (
                SELECT
                    tx_hash
                FROM
                    safe_transfers
            )
    ),
    labelled_transfers AS (
        SELECT
            t.blockchain,
            t.block_date,
            t.block_time,
            t.tx_hash,
            CASE
                WHEN stb.stablecoin_address IS NOT NULL THEN 'stablecoin'
                ELSE token_standard
            END AS token_category,
            "from" AS safe_sender,
            to AS receiver,
            CASE
                WHEN to IN (
                    SELECT
                        address
                    FROM
                        safe_ethereum.safes
                ) THEN 'Safe'
                WHEN to IN (
                    SELECT
                        address
                    FROM
                        labels.burn_addresses
                ) THEN 'Burner address'
                WHEN to IN (
                    SELECT
                        address
                    FROM
                        ethereum.creation_traces
                ) THEN 'Smart contract'
                ELSE 'EOA'
            END AS receiver_type,
            CASE
                WHEN l.contract_name LIKE '%Gnosis_safe%' THEN 'Safe'
                ELSE l.contract_name
            END AS receiver_contract_name,
            l.label_names AS receiver_label_names,
            l.label_categories AS receiver_label_categories,
            l.label_models AS receiver_label_models,
            l.ens AS receiver_ens,
            CASE
                WHEN l.label_models = 'dao_multisig' THEN CONCAT('Safe: ', SPLIT_PART(l.label_names, ':', 1)) --if it's a DAO safe, get the name from labels (because contract name always = GnosisSafe)
                ELSE COALESCE(
                    SPLIT_PART(l.contract_name, ': ', 1),
                    l.label_names,
                    l.ens
                )
            END AS receiver_name, --get name from contract name if available, otherwise from labels
            e.event_name,
            symbol,
            contract_address AS token_address,
            amount,
            amount_usd
        FROM
            safe_transfers t
            LEFT JOIN dune.safe.dataset_stablecoin_addresses_by_chain stb ON t.blockchain = stb.blockchain
            AND t.contract_address = stb.stablecoin_address
            LEFT JOIN receiver_labels l ON t.to = l.address
            LEFT JOIN evt_names e ON t.tx_hash = e.tx_hash
            AND t.evt_index = e.index
        WHERE
            amount_usd IS NOT NULL
    ),
    labelled_transfers_with_vertical AS (
        SELECT
            *,
            CASE
                WHEN receiver_label_models = 'burn_addresses' THEN 'burn address'
                WHEN receiver_contract_name = 'Myname: WETH9' THEN 'eth wrapping'
                WHEN receiver_label_models = 'cex_ethereum' THEN 'CEX'
                WHEN receiver_label_categories = 'bridge'
                OR receiver_name IN (
                    'Across_v2',
                    'Hop_protocol',
                    'Stargate',
                    'Stargate_v2',
                    'Zklink'
                ) THEN 'bridge'
                WHEN receiver_label_categories = 'dex'
                OR receiver_name IN (
                    'Balancer_v2',
                    'Curvefi',
                    'Curve',
                    'Gnosis_protocol_v2',
                    'Lifi',
                    'Oneinch',
                    'Sushi'
                )
                OR (
                    receiver_type = 'Smart contract'
                    AND lower(receiver_name) LIKE '%swap%'
                ) --includes: paraswap, swapr, uniswap, shibaswap,defiswap and more
                THEN 'DEX'
                WHEN receiver_name IN (
                    'Aave',
                    'Aave_v2',
                    'Aave_v3',
                    'Clearpool_finance',
                    'Clearpool',
                    'Compound_v2',
                    'Compound_v3',
                    'Curve_lend',
                    'Echelon',
                    'Euler',
                    'Fluid',
                    'Fluxfinance',
                    'Fraxfinance',
                    'Maplefinance_v2',
                    'Midas',
                    'Morpho',
                    'Morpho_blue',
                    'Morpho_aave_v2',
                    'Morpho_compound',
                    'Silo',
                    'Spark_protocol',
                    'Uwulend',
                    'Yearn'
                ) THEN 'lending'
                WHEN receiver_name IN (
                    'Etherfi',
                    'Etherfiliquiditypool',
                    'Frax',
                    'Lido',
                    'Mantle',
                    'Meveth',
                    'Pirex',
                    'Rocketpool',
                    'Rockx_liquid_staking',
                    'Stader',
                    'Stakewise',
                    'Stakewise_v3',
                    'Swell_v3'
                ) THEN 'liquid staking'
                WHEN receiver_name IN ('Eigenlayer', 'Symbiotic') THEN 'restaking'
                WHEN receiver_name IN ('Kelpdao', 'Mellow_lrt', 'Renzo') THEN 'liquid restaking' --https://defillama.com/protocols/Liquid%20Restaking
                WHEN receiver_type = 'Smart contract'
                AND receiver IN (
                    SELECT
                        address
                    FROM
                        labels.eth_stakers
                ) THEN 'ETH staking' --captures the likes of Kiln, abyss, Stakefish
                WHEN receiver_name IN (
                    'Aura_finance',
                    'Apecoin',
                    'Convex',
                    'Ethena_labs',
                    'Eth_fox_vault',
                    'Instadapp_lite',
                    'Origin_protocol',
                    'Pendle',
                    'Redacted',
                    'Tokemak',
                    'Usual'
                ) THEN 'yield'
                WHEN receiver_name IN (
                    'Abracadabra',
                    'Amp',
                    'Lybra_finance',
                    'Maker',
                    'Threshold_network'
                ) THEN 'CDP'
                WHEN receiver_name IN ('Contango_v2', 'Dydx') THEN 'derivatives'
                WHEN receiver_name IN ('Anzen_finance_v2') THEN 'rwa' --https://defillama.com/protocols/RWA
                WHEN receiver_name IN ('Zircuit_staking') THEN 'farm' --https://defillama.com/protocols/farm
                WHEN receiver_name IN ('Llamapay') THEN 'payments'
                ELSE CONCAT('unclassified transfer to ', receiver_type)
            END AS vertical
        FROM
            labelled_transfers
    ),
    labelled_transfers_with_vertical_and_protocol AS (
        SELECT
            *,
            COALESCE(
                receiver_contract_name,
                CONCAT('unclassified transfer to ', receiver_type)
            ) AS protocol
        FROM
            labelled_transfers_with_vertical
    )
SELECT
    *
FROM
    labelled_transfers_with_vertical_and_protocol
order by
    block_date