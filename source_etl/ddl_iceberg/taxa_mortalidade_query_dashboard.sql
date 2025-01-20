WITH dados_nascimento AS (
    SELECT 
        nasc_loc.county AS estado,                                                    -- unidade federativa de registro do nascimento
        COUNT(*) AS nascimentos_vivos 
    FROM 
        bios.rebios.person sus,                                                     -- tabela com os registros de nascimentos e óbitos
        bios.rebios.datasus_person nasc_sus,                                         -- tabela com o registro de nascimentos
        bios.rebios.location nasc_loc                                                -- tabela de localizações (endreços e cidades)
    WHERE 
        EXTRACT(YEAR FROM birth_timestamp) = 2019                                   -- data e hora de nascimento
        and sus.person_id = nasc_sus.person_id                                      -- join para obter os registros de nascimentos sem os registros de óbito
        and sus.location_id = nasc_loc.location_id                                  -- join para obter a UF do nascimento
    GROUP BY 
        nasc_loc.county
),
dados_obitos AS (
    SELECT 
        obit_loc.county AS estado,                                                    -- unidade federativa de registro do óbito
        COUNT(CASE WHEN date_diff(obit_sus.death_timestamp, sus.birth_timestamp) <= 365 THEN 1 END) AS obitos_0_1,    -- data e hora do óbito e data e hora de nascimento
        COUNT(CASE WHEN date_diff(obit_sus.death_timestamp, sus.birth_timestamp) <= 1825 THEN 1 END) AS obitos_0_5    -- data e hora do óbito e data e hora de nascimento
    FROM 
        bios.rebios.person sus,                                                      -- tabela com os registros de nascimentos e óbitos
        bios.rebios.death obit_sus,                                                  -- tabela com o registro de óbitos
        bios.rebios.location obit_loc                                                -- tabela de localizações (endreços e cidades)
    WHERE 
        EXTRACT(YEAR FROM obit_sus.death_timestamp) = 2019
        and sus.person_id = obit_sus.person_id                                      -- join para obter os registros de nascimentos sem os registros de óbito
        and sus.location_id = obit_loc.location_id                                  -- join para obter a UF do nascimento
    GROUP BY 
        obit_loc.county
)
SELECT 
    CASE 
        WHEN dn.estado = '11' THEN 'BR-RO'
        WHEN dn.estado = '12' THEN 'BR-AC'
        WHEN dn.estado = '13' THEN 'BR-AM'
        WHEN dn.estado = '14' THEN 'BR-RR'
        WHEN dn.estado = '15' THEN 'BR-PA'
        WHEN dn.estado = '16' THEN 'BR-AP'
        WHEN dn.estado = '17' THEN 'BR-TO'
        WHEN dn.estado = '21' THEN 'BR-MA'
        WHEN dn.estado = '22' THEN 'BR-PI'
        WHEN dn.estado = '23' THEN 'BR-CE'
        WHEN dn.estado = '24' THEN 'BR-RN'
        WHEN dn.estado = '25' THEN 'BR-PB'
        WHEN dn.estado = '26' THEN 'BR-PE'
        WHEN dn.estado = '27' THEN 'BR-AL'
        WHEN dn.estado = '28' THEN 'BR-SE'
        WHEN dn.estado = '29' THEN 'BR-BA'
        WHEN dn.estado = '31' THEN 'BR-MG'
        WHEN dn.estado = '32' THEN 'BR-ES'
        WHEN dn.estado = '33' THEN 'BR-RJ'
        WHEN dn.estado = '35' THEN 'BR-SP'
        WHEN dn.estado = '41' THEN 'BR-PR'
        WHEN dn.estado = '42' THEN 'BR-SC'
        WHEN dn.estado = '43' THEN 'BR-RS'
        WHEN dn.estado = '50' THEN 'BR-MS'
        WHEN dn.estado = '51' THEN 'BR-MT'
        WHEN dn.estado = '52' THEN 'BR-GO'
        WHEN dn.estado = '53' THEN 'BR-DF'
        ELSE 'Desconhecido'
    END AS iso_estado,
    ROUND(COALESCE((dob.obitos_0_1 * 1000.0) / dn.nascimentos_vivos, 0), 2) AS taxa_mortalidade_0_1_ano,
    ROUND(COALESCE((dob.obitos_0_5 * 1000.0) / dn.nascimentos_vivos, 0), 2) AS taxa_mortalidade_0_5_anos,
    CASE 
        WHEN ROUND(COALESCE((dob.obitos_0_1 * 1000.0) / dn.nascimentos_vivos, 0), 2) <= 11 THEN '< 11,0'
        WHEN ROUND(COALESCE((dob.obitos_0_1 * 1000.0) / dn.nascimentos_vivos, 0), 2) BETWEEN 11 AND 13.0 THEN '11,1 - 13,1'
        WHEN ROUND(COALESCE((dob.obitos_0_1 * 1000.0) / dn.nascimentos_vivos, 0), 2) BETWEEN 13 AND 16.0 THEN '13,1 - 16,0'
        WHEN ROUND(COALESCE((dob.obitos_0_1 * 1000.0) / dn.nascimentos_vivos, 0), 2) BETWEEN 16.1 AND 19.9 THEN '16,1 - 19,9'
        WHEN ROUND(COALESCE((dob.obitos_0_1 * 1000.0) / dn.nascimentos_vivos, 0), 2) > 19.9 THEN '19,9 >'
        ELSE 'Fora do intervalo'
    END AS categoria_mortalidade_0_1_ano,
    CASE 
        WHEN ROUND(COALESCE((dob.obitos_0_5 * 1000.0) / dn.nascimentos_vivos, 0), 2) < 11 THEN '< 11,0'
        WHEN ROUND(COALESCE((dob.obitos_0_5 * 1000.0) / dn.nascimentos_vivos, 0), 2) BETWEEN 11 AND 13.0 THEN '11,1 - 13,0'
        WHEN ROUND(COALESCE((dob.obitos_0_5 * 1000.0) / dn.nascimentos_vivos, 0), 2) BETWEEN 13 AND 16.0 THEN '13,1 - 16,0'
        WHEN ROUND(COALESCE((dob.obitos_0_5 * 1000.0) / dn.nascimentos_vivos, 0), 2) BETWEEN 16.1 AND 19.9 THEN '16,1 - 19,9'
        WHEN ROUND(COALESCE((dob.obitos_0_5 * 1000.0) / dn.nascimentos_vivos, 0), 2) > 19.9 THEN '19,9 >'
        ELSE 'Fora do intervalo'
    END AS categoria_mortalidade_0_5_anos,
        CASE 
        WHEN ROUND(COALESCE((dob.obitos_0_1 * 1000.0) / dn.nascimentos_vivos, 0), 2) <= 11 THEN 1
        WHEN ROUND(COALESCE((dob.obitos_0_1 * 1000.0) / dn.nascimentos_vivos, 0), 2) BETWEEN 11 AND 13.0 THEN 2
        WHEN ROUND(COALESCE((dob.obitos_0_1 * 1000.0) / dn.nascimentos_vivos, 0), 2) BETWEEN 13 AND 16.0 THEN 3
        WHEN ROUND(COALESCE((dob.obitos_0_1 * 1000.0) / dn.nascimentos_vivos, 0), 2) BETWEEN 16.1 AND 19.9 THEN 4
        WHEN ROUND(COALESCE((dob.obitos_0_1 * 1000.0) / dn.nascimentos_vivos, 0), 2) > 19.9 THEN 5
    END AS categoria_mortalidade_0_1_ano_num,
    CASE 
        WHEN ROUND(COALESCE((dob.obitos_0_5 * 1000.0) / dn.nascimentos_vivos, 0), 2) < 11 THEN 1
        WHEN ROUND(COALESCE((dob.obitos_0_5 * 1000.0) / dn.nascimentos_vivos, 0), 2) BETWEEN 11 AND 13.0 THEN 2
        WHEN ROUND(COALESCE((dob.obitos_0_5 * 1000.0) / dn.nascimentos_vivos, 0), 2) BETWEEN 13 AND 16.0 THEN 3
        WHEN ROUND(COALESCE((dob.obitos_0_5 * 1000.0) / dn.nascimentos_vivos, 0), 2) BETWEEN 16.1 AND 19.9 THEN 4
        WHEN ROUND(COALESCE((dob.obitos_0_5 * 1000.0) / dn.nascimentos_vivos, 0), 2) > 19.9 THEN 5
    END AS categoria_mortalidade_0_5_anos_num,
    "0 a 1",
    "0 a 5"
FROM 
    dados_nascimento dn
LEFT JOIN 
    dados_obitos dob 
    ON dn.estado = dob.estado
ORDER BY 
    iso_estado